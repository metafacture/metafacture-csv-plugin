/*
 * Copyright 2018 Deutsche Nationalbibliothek
 *
 * Licensed under the Apache License, Version 2.0 the "License";
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.metafacture.csv;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.metafacture.framework.StreamReceiver;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.inOrder;

public class CsvLiteralDecoderTest {

    private CsvLiteralDecoder decoder;

    @Mock
    private StreamReceiver receiver;

    @Before
    public void setupUp() {
        MockitoAnnotations.initMocks(this);
        decoder = new CsvLiteralDecoder(',');
        decoder.setReceiver(receiver);
    }

    @After
    public void tearDown() {
        decoder.closeStream();
    }

    @Test
    public void readLine() {
        decoder.process("a,b,c");

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("1");
        ordered.verify(receiver).literal("1", "a");
        ordered.verify(receiver).literal("2", "b");
        ordered.verify(receiver).literal("3", "c");
        ordered.verify(receiver).endRecord();
    }

    @Test
    public void readTwoLines() {
        decoder.process("a");
        decoder.process("b");

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("1");
        ordered.verify(receiver).literal("1", "a");
        ordered.verify(receiver).endRecord();

        ordered.verify(receiver).startRecord("2");
        ordered.verify(receiver).literal("1", "b");
        ordered.verify(receiver).endRecord();
    }

    @Test
    public void readLineWithQuotes() {
        decoder.process("\"a\",\"b\"");

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).startRecord("1");
        ordered.verify(receiver).literal("1", "a");
        ordered.verify(receiver).literal("2", "b");
        ordered.verify(receiver).endRecord();
    }
}