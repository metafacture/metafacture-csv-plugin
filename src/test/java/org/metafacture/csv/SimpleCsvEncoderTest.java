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

import org.junit.Before;
import org.junit.Test;
import org.metafacture.framework.ObjectReceiver;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.inOrder;

public class SimpleCsvEncoderTest {

    private SimpleCsvEncoder encoder;

    @Mock
    private ObjectReceiver<String> receiver;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        encoder = new SimpleCsvEncoder(' ');
        encoder.setReceiver(receiver);
    }

    @Test
    public void shouldReceiveSingleRecord() {
        encoder.startRecord("1");
        encoder.literal("column 1", "a");
        encoder.literal("column 2", "b");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'a' 'b'"));
    }

    @Test
    public void shouldReceiveSingleRecordWithHeader() {
        encoder.setIncludeHeader(true);

        encoder.startRecord("1");
        encoder.literal("column 1", "a");
        encoder.literal("column 2", "b");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'column 1' 'column 2'"));
        ordered.verify(receiver).process(ticksToQuotes("'a' 'b'"));
    }

    @Test
    public void shouldReceiveSingleRecordWithRecordId() {
        encoder.setIncludeRecordId(true);

        encoder.startRecord("1");
        encoder.literal("column 1", "a");
        encoder.literal("column 2", "b");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'1' 'a' 'b'"));
    }

    @Test
    public void shouldReceiveSingleRecordWithRecordIdAndHeader() {
        encoder.setIncludeRecordId(true);
        encoder.setIncludeHeader(true);

        encoder.startRecord("1");
        encoder.literal("column 1", "a");
        encoder.literal("column 2", "b");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'record id' 'column 1' 'column 2'"));
        ordered.verify(receiver).process(ticksToQuotes("'1' 'a' 'b'"));
    }


    @Test
    public void shouldReceiveThreeRows() {
        encoder.startRecord("1");
        encoder.literal("column 1", "a");
        encoder.literal("column 2", "b");
        encoder.endRecord();
        encoder.startRecord("2");
        encoder.literal("column 1", "c");
        encoder.literal("column 2", "d");
        encoder.endRecord();
        encoder.startRecord("3");
        encoder.literal("column 1", "e");
        encoder.literal("column 2", "f");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'a' 'b'"));
        ordered.verify(receiver).process(ticksToQuotes("'c' 'd'"));
        ordered.verify(receiver).process(ticksToQuotes("'e' 'f'"));
    }

    @Test
    public void shouldUseCommaAsSeparator() {
        encoder.setSeparator(',');

        encoder.startRecord("1");
        encoder.literal("column 1", "a");
        encoder.literal("column 2", "b");
        encoder.endRecord();
        encoder.startRecord("1");
        encoder.literal("column 1", "c");
        encoder.literal("column 2", "d");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'a','b'"));
        ordered.verify(receiver).process(ticksToQuotes("'c','d'"));
    }

    @Test
    public void shouldCreateNestedCsvInColumn() {
        encoder.startRecord("1");
        encoder.literal("name", "a");
        encoder.literal("alias", "a1");
        encoder.literal("alias", "a2");
        encoder.literal("alias", "a3");
        encoder.endRecord();
        encoder.closeStream();

        final InOrder ordered = inOrder(receiver);
        ordered.verify(receiver).process(ticksToQuotes("'a' '''a1'' ''a2'' ''a3'''"));
    }

    private String ticksToQuotes(String s) {
        return s.replace("'", "\"");
    }
}