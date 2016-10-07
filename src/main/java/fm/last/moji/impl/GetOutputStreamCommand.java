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
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.moji.tracker.Destination;
import fm.last.moji.tracker.Tracker;
import fm.last.moji.tracker.TrackerException;
import fm.last.moji.tracker.TrackerFactory;

class GetOutputStreamCommand implements MojiCommand {

  final String key;
  final String domain;
  final String storageClass;
  final int quorumSize;
  private final TrackerFactory trackerFactory;
  private final HttpConnectionFactory httpFactory;
  private OutputStream stream;
  private final Lock writeLock;

  GetOutputStreamCommand(TrackerFactory trackerFactory, HttpConnectionFactory httpFactory, String key, String domain,
    String storageClass, boolean durableWrite, Lock writeLock) {
    this.trackerFactory = trackerFactory;
    this.httpFactory = httpFactory;
    this.key = key;
    this.domain = domain;
    this.storageClass = storageClass;
    this.quorumSize = durableWrite ? 2:1;
    this.writeLock = writeLock;
  }

  @Override
  public void executeWithTracker(Tracker tracker) throws IOException {
    List<Destination> destinations = tracker.createOpen(key, domain, storageClass);
    if (destinations.size() < quorumSize) {
      throw new TrackerException("Failed to obtain sufficient destinations for domain=" + domain + ",key=" + key
          + ",storageClass=" + storageClass);
    }

    try {
      stream = new FileUploadOutputStream(trackerFactory, httpFactory, key, domain, destinations,
        quorumSize, writeLock);
    } catch (IOException e) {
      IOUtils.closeQuietly(stream);
      throw e;
    }
  }

  OutputStream getOutputStream() {
    return stream;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("GetOutputStreamCommand [domain=");
    builder.append(domain);
    builder.append(", key=");
    builder.append(key);
    builder.append(", storageClass=");
    builder.append(storageClass);
    builder.append(", durableWrite=");
    builder.append(quorumSize > 1);
    builder.append("]");
    return builder.toString();
  }

}
