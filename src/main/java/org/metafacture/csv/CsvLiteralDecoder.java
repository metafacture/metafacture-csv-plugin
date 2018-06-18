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

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultObjectPipe;

import java.io.IOException;

/**
 * <p>Decoder that reads a csv line. Comma is used as default separator.</p>
 * <p>Each line gets decomposed into a record with the following structure:</p>
 *
 * <ul>
 *     <li>Record
 *     <ul><li>Identifier: Row number</li></ul>
 *     </li>
 *     <li>Literal
 *     <ul>
 *         <li>Name: Column number</li>
 *         <li>Value: Column content</li>
 *     </ul>
 *     </li>
 * </ul>
 */
@Description("Decodes lines of CSV files. Each row is a record. Each column a literal value.")
@In(String.class)
@Out(StreamReceiver.class)
@FluxCommand("decode-literal-csv")
public class CsvLiteralDecoder extends DefaultObjectPipe<String, StreamReceiver> {

    private char separator;
    private boolean onStart;
    private CSVParser parser;
    private int rowIdx;

    public CsvLiteralDecoder() {
        this(CSVWriter.DEFAULT_SEPARATOR);
    }

    public CsvLiteralDecoder(char separator) {
        this.separator = separator;
        this.onStart = true;
        this.rowIdx = 0;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    @Override
    public void process(String csv) {
        if (onStart) {
            parser = new CSVParserBuilder()
                    .withSeparator(separator)
                    .withIgnoreQuotations(false)
                    .build();
            onStart = false;
        }

        try {
            String[] line = parser.parseLine(csv);
            getReceiver().startRecord(String.valueOf(++rowIdx));
            for (int columnIdx = 0; columnIdx < line.length; columnIdx++) {
                getReceiver().literal(String.valueOf(columnIdx + 1), line[columnIdx]);
            }
            getReceiver().endRecord();
        } catch (IOException e) {
            throw new MetafactureException(e);
        }
    }

    @Override
    public void onResetStream() {
        this.rowIdx = 0;
    }
}
