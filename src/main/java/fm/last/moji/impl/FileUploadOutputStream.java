/*
 * Copyright 2012 Last.fm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.moji.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.output.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.moji.tracker.Destination;
import fm.last.moji.tracker.Tracker;
import fm.last.moji.tracker.TrackerException;
import fm.last.moji.tracker.TrackerFactory;

class FileUploadOutputStream extends OutputStream {

  private static final Logger log = LoggerFactory.getLogger(FileUploadOutputStream.class);

  private static final int CHUNK_LENGTH = 4096;
  private final List<Destination> destinations;
  private final int quorumSize;
  private final TrackerFactory trackerFactory;
  private final String key;
  private final String domain;
  private final Lock writeLock;
  private final List<HttpURLConnection> httpConnections;
  private final List<CountingOutputStream> delegates;
  private long size = -1L;

  FileUploadOutputStream(TrackerFactory trackerFactory, HttpConnectionFactory httpFactory, String key, String domain,
    Destination destination, Lock writeLock) throws IOException {
    this(trackerFactory, httpFactory, key, domain, Arrays.asList(destination), 1, writeLock);
  }

  FileUploadOutputStream(TrackerFactory trackerFactory, HttpConnectionFactory httpFactory, String key, String domain,
      List<Destination> candidates, int quorumSize, Lock writeLock) throws IOException {
    this.trackerFactory = trackerFactory;
    this.domain = domain;
    this.key = key;
    this.writeLock = writeLock;
    this.quorumSize = quorumSize;
    this.destinations = new ArrayList<Destination>();
    this.httpConnections = new ArrayList<HttpURLConnection>();
    this.delegates = new ArrayList<CountingOutputStream>();

    initStoreConnection(httpFactory, candidates);
  }

  /**
   * Initial appropriate sized destinations, httpConnections and delegates according to quorumSize
   */
  private void initStoreConnection(HttpConnectionFactory httpFactory, List<Destination> candidates)
    throws IOException {
    for (Destination candidate : candidates) {
      log.debug("Creating output stream to: {}", candidate);
      try {
        log.debug("HTTP PUT -> opening chunked stream -> {}", candidate.getPath());
        HttpURLConnection httpConnection = httpFactory.newConnection(candidate.getPath());
        httpConnection.setRequestMethod("PUT");
        httpConnection.setChunkedStreamingMode(CHUNK_LENGTH);
        httpConnection.setDoOutput(true);
        CountingOutputStream delegate = new CountingOutputStream(httpConnection.getOutputStream());
        this.destinations.add(candidate);
        this.httpConnections.add(httpConnection);
        this.delegates.add(delegate);
      } catch (IOException e) {
        log.debug("Failed to open output -> {}", candidate);
        log.debug("Exception was: ", e);
      }
      if (this.destinations.size() == quorumSize) {
        break;
      }
    }

    if (this.httpConnections.size() < quorumSize) {
      throw new TrackerException("Failed to connect to sufficient destinations for domain=" + domain + ",key=" + key);
    }
  }

  @Override
  public void write(int b) throws IOException {
    for (CountingOutputStream delegate:delegates) {
      delegate.write(b);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    for (CountingOutputStream delegate:delegates) {
      delegate.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    for (CountingOutputStream delegate:delegates) {
      delegate.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    for (CountingOutputStream delegate:delegates) {
      delegate.flush();
    }
  }

  @Override
  public void close() throws IOException {
    log.debug("Close called on {}", this);
    try {
      flushAndClose();
      trackerCreateClose();
    } finally {
      unlockQuietly(writeLock);
    }
  }

  /**
   * Send create_close command to tracker to finish mogilefs file write procedure
   *
   * @throws TrackerException If the create_close command fails.
   */
  private void trackerCreateClose() throws TrackerException {
    /*
     * Fixed the maxAttempts = 2 so the behavior is just retry once. If there is only one tracker, it gives the tracker
     * a second chance. If there are multiple trackers, it just tries one other tracker, but does not waste time trying
     * all available trackers.
     */
    int maxAttempts = 2;
    TrackerException lastException = null;
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      Tracker tracker = null;
      try {
        tracker = trackerFactory.getTracker();
        // Compatible to old version mogilefs clusters that support only classical create_close request.
        if (destinations.size() == 1) {
          tracker.createClose(key, domain, destinations.get(0), size);
        } else {
          tracker.createClose(key, domain, destinations, size);
        }
        return;
      } catch (TrackerException e) {
        lastException = e;
        /*
         * Call attention to the user. User should diagnose the issue as soon as possible to prevent additional latency
         * caused by retry, or before all trackers are down.
         */
        log.warn("create_close attempt {} failed", attempt + 1, e);
      } finally {
        if (tracker != null) {
          tracker.close();
        }
      }
    }

    log.error("All {} attempts to create_close failed", maxAttempts);
    throw lastException;
  }

  /**
   * Guarantee that all connections to storage node will be closed.
   *
   * @throws IOException the last occurred exception
   */
  private void flushAndClose() throws IOException {
    IOException lastException = null;
    for (int i = 0; i < quorumSize; i++) {
      try {
        flushAndClose(delegates.get(i), httpConnections.get(i));
      } catch (Exception e) {
        if (e instanceof IOException) {
          lastException = (IOException) e;
        } else {
          lastException = new IOException(e);
        }
      }
    }
    if (lastException != null) {
      throw lastException;
    }
  }

  private void flushAndClose(CountingOutputStream delegate, HttpURLConnection httpConnection) throws IOException {
    try {
      delegate.flush();
      long size = delegate.getByteCount();
      log.debug("Bytes written: {} to {}", size, httpConnection);
      if (this.size != -1 && this.size != size) {
        throw new IOException("Bytes written size mismatch: " + this.size + " and " + size);
      }
      this.size = size;
      int code = httpConnection.getResponseCode();
      if (HttpURLConnection.HTTP_OK != code && HttpURLConnection.HTTP_CREATED != code) {
        String message = httpConnection.getResponseMessage();
        throw new IOException(
            "HTTP Error during flush: " + code + ", " + message + ", peer: '{" + httpConnection + "}'");
      }
    } finally {
      try {
        delegate.close();
      } catch (Exception e) {
        log.warn("Error closing stream", e);
      }
      try {
        httpConnection.disconnect();
      } catch (Exception e) {
        log.warn("Error closing connection", e);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("FileUploadOutputStream [domain=");
    builder.append(domain);
    builder.append(", key=");
    builder.append(key);
    builder.append(", destination=");
    builder.append(destinations);
    builder.append("]");
    return builder.toString();
  }

  private void unlockQuietly(Lock lock) {
    try {
      lock.unlock();
    } catch (IllegalMonitorStateException e) {
    }
  }

}
