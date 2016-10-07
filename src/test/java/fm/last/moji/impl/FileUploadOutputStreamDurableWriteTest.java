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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fm.last.moji.tracker.Destination;
import fm.last.moji.tracker.Tracker;
import fm.last.moji.tracker.TrackerException;
import fm.last.moji.tracker.TrackerFactory;

@RunWith(MockitoJUnitRunner.class)
public class FileUploadOutputStreamDurableWriteTest {

  private static final String KEY = "key";
  private static final String DOMAIN = "domain";
  @Mock
  private TrackerFactory mockTrackerFactory;
  @Mock
  private HttpConnectionFactory mockHttpFactory;
  @Mock
  private Destination mockDestination, mockDestination2;
  @Mock
  private HttpURLConnection mockHttpConnection, mockHttpConnection2;
  @Mock
  private OutputStream mockOutputStream, mockOutputStream2;
  @Mock
  private InputStream mockInputStream, mockInputStream2;
  @Mock
  private Tracker mockTracker;
  @Mock
  private Lock mockWriteLock;
  private FileUploadOutputStream stream;

  private List<HttpURLConnection> mockHttpConnections;
  private List<OutputStream> mockOutputStreams;
  private List<InputStream> mockInputStreams;
  private List<Destination> mockDestinations;

  @Before
  public void setUp() throws IOException {
    URL url = new URL("http://www.last.fm/");
    URL url2 = new URL("http://www.last.fm/2");
    when(mockDestination.getPath()).thenReturn(url);
    when(mockDestination2.getPath()).thenReturn(url2);
    when(mockHttpFactory.newConnection(url)).thenReturn(mockHttpConnection);
    when(mockHttpFactory.newConnection(url2)).thenReturn(mockHttpConnection2);
    when(mockHttpConnection.getInputStream()).thenReturn(mockInputStream);
    when(mockHttpConnection.getOutputStream()).thenReturn(mockOutputStream);
    when(mockHttpConnection.getResponseMessage()).thenReturn("message");
    when(mockHttpConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    when(mockHttpConnection2.getInputStream()).thenReturn(mockInputStream2);
    when(mockHttpConnection2.getOutputStream()).thenReturn(mockOutputStream2);
    when(mockHttpConnection2.getResponseMessage()).thenReturn("message");
    when(mockHttpConnection2.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    when(mockTrackerFactory.getTracker()).thenReturn(mockTracker);

    mockHttpConnections = Arrays.asList(mockHttpConnection, mockHttpConnection2);
    mockOutputStreams = Arrays.asList(mockOutputStream, mockOutputStream2);
    mockInputStreams = Arrays.asList(mockInputStream, mockInputStream2);
    mockDestinations =  Arrays.asList(mockDestination, mockDestination2);

    stream = new FileUploadOutputStream(mockTrackerFactory, mockHttpFactory, KEY, DOMAIN, mockDestinations,
        2, mockWriteLock);
  }

  @Test
  public void httpConnectionSetUp() throws IOException {
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      verify(mockHttpConnection).setRequestMethod("PUT");
      verify(mockHttpConnection).setChunkedStreamingMode(4096);
      verify(mockHttpConnection).setDoOutput(true);
    }
  }

  @Test
  public void everyThingCloses() throws IOException {
    stream.write(1);
    stream.close();

    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).flush();
      verify(mockOutputStream).close();
    }
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      verify(mockHttpConnection).disconnect();
    }
    verify(mockTracker).createClose(KEY, DOMAIN, mockDestinations, 1);
    verify(mockTracker).close();
    verify(mockWriteLock).unlock();
  }

  @Test
  public void everyThingClosesEvenOnFail() throws IOException {
    for (OutputStream mockOutputStream: mockOutputStreams) {
      doThrow(new RuntimeException()).when(mockOutputStream).flush();
      doThrow(new RuntimeException()).when(mockOutputStream).close();
    }
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      when(mockHttpConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
      doThrow(new RuntimeException()).when(mockHttpConnection).disconnect();
    }
    for (InputStream mockInputStream: mockInputStreams) {
      doThrow(new RuntimeException()).when(mockInputStream).close();
    }
    doThrow(new RuntimeException()).when(mockTracker).createClose(KEY, DOMAIN, mockDestinations, 1);

    try {
      stream.write(1);
      stream.close();
    } catch (Exception e) {
    }

    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).flush();
      verify(mockOutputStream).close();
    }
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      verify(mockHttpConnection).disconnect();
    }
    verify(mockWriteLock).unlock();
  }

  @Test(expected = NullPointerException.class)
  public void nullTrackerFromFactory() throws IOException {
    when(mockTrackerFactory.getTracker()).thenReturn(null);

    stream.write(1);
    stream.close();
  }

  @Test
  public void trackerClosesOnFailSecondAttemptSucceeds() throws IOException {
    doThrow(new TrackerException()).doNothing().when(mockTracker).createClose(KEY, DOMAIN, mockDestinations, 1);

    stream.write(1);
    stream.close();

    verify(mockTrackerFactory, times(2)).getTracker();
    verify(mockTracker, times(2)).close();
  }

  @Test
  public void trackerClosesOnFailMaxAttempts() throws IOException {
    doThrow(new TrackerException()).when(mockTracker).createClose(KEY, DOMAIN, mockDestinations, 1);

    try {
      stream.write(1);
      stream.close();
      fail("Exception expected");
    } catch (TrackerException e) {
    }

    verify(mockTrackerFactory, times(2)).getTracker();
    verify(mockTracker, times(2)).close();
  }

  @Test
  public void flushResponseCodeCreated() throws IOException {
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      when(mockHttpConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_CREATED);
    }

    stream.write(1);
    stream.close();

    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).write(1);
    }
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      verify(mockHttpConnection).disconnect();
    }
  }

  @Test
  public void flushResponseCodeNotOKOrCreated() throws IOException {
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      when(mockHttpConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAVAILABLE);
    }

    stream.write(1);
    try {
      stream.close();
      fail("IOException should be thrown");
    } catch (IOException e) {
    }

    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      verify(mockHttpConnection).disconnect();
    }
  }

  @Test
  public void flushAndDisconnectError() throws IOException {
    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      doThrow(new RuntimeException()).when(mockHttpConnection).disconnect();
    }

    stream.write(1);
    stream.close();

    for (HttpURLConnection mockHttpConnection: mockHttpConnections) {
      verify(mockHttpConnection).disconnect();
    }
  }

  @Test
  public void writeIntDelegates() throws IOException {
    stream.write(1);
    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).write(1);
    }
  }

  @Test
  public void writeByteArrayDelegates() throws IOException {
    byte[] b = new byte[4];
    stream.write(b);
    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).write(b);
    }
  }

  @Test
  public void writeByteArrayWithOffsetDelegates() throws IOException {
    byte[] b = new byte[4];
    stream.write(b, 2, 4);
    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).write(b, 2, 4);
    }
  }

  @Test
  public void flushDelegates() throws IOException {
    stream.flush();
    for (OutputStream mockOutputStream: mockOutputStreams) {
      verify(mockOutputStream).flush();
    }
  }

  @Test
  public void countOnWrite() throws IOException {
    byte[] b = new byte[] { 1, 2, 3, 4, 5 };
    stream.write(b);
    stream.flush();
    stream.close();

    verify(mockTracker).createClose(KEY, DOMAIN, mockDestinations, 5);
  }

}
