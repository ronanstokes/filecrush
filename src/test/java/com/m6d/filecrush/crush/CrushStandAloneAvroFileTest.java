/*
   Copyright 2011 m6d.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.m6d.filecrush.crush;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Dfs block size will be set to 50 and threshold set to 20%.
 */
@SuppressWarnings("deprecation")
public class CrushStandAloneAvroFileTest {
	@Rule
	public static TemporaryFolder tmp = new TemporaryFolder();

	private JobConf job;

    private String avroSchema = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Pair\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"key\", \"type\": \"string\"},\n" +
            "     {\"name\": \"value\",  \"type\": [\"int\", \"null\"]}\n" +
            " ]\n" +
            "}";

    private Schema schema = new Parser().parse(avroSchema);

	@Before
	public void setup() throws Exception {
		job = new JobConf(false);

		job.set("fs.default.name", "file:///");
		job.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		job.setLong("dfs.block.size", 50);
	}

    /**
     * We are using JUnit temporary directory and a Rule to get rid of the directory when we are done
     * JUnit calls "after" before removing the temp directory
     * So we use AfterClass to validate the directory is gone
     */
    @AfterClass
    public static void deleteTmp() throws IOException {
        assertThat(tmp.getRoot().exists(),is(false));
    }

	@Test
	public void standAloneOutput() throws Exception {

        String schemaFile = createSchemaFile(tmp.newFolder("avro"),"schema.avsc");

        File in = tmp.newFolder("in");

		createFile(in, "skipped-0.avro", 0, 25);
		createFile(in, "skipped-1.avro", 1, 25);
		createFile(in, "skipped-2.avro", 2, 25);
		createFile(in, "skipped-3.avro", 3, 25);

		File subdir = tmp.newFolder("in/subdir");

		createFile(subdir, "lil-0.avro", 0, 1);
		createFile(subdir, "lil-1.avro", 1, 2);
		createFile(subdir, "big-2.avro", 2, 5);
		createFile(subdir, "big-3.avro", 3, 5);

		File subsubdir = tmp.newFolder("in/subdir/subsubdir");

		createFile(subsubdir, "skipped-4.avro", 4, 25);
		createFile(subsubdir, "skipped-5.avro", 5, 25);

		File out = new File(tmp.getRoot(), "out");

        ToolRunner.run(job, new Crush(), new String[] {
                "--input-format=avro",
                "--output-format=avro",
                "--compress=none",
                "--avro-schema-file="+schemaFile,
                subdir.getAbsolutePath(), out.getAbsolutePath()
        });

		/*
		 * Make sure the original files are still there.
		 */
		verifyFile(in, "skipped-0.avro", 0, 25);
		verifyFile(in, "skipped-1.avro", 1, 25);
		verifyFile(in, "skipped-2.avro", 2, 25);
		verifyFile(in, "skipped-3.avro", 3, 25);

		verifyFile(subdir, "lil-0.avro", 0, 1);
		verifyFile(subdir, "lil-1.avro", 1, 2);
		verifyFile(subdir, "big-2.avro", 2, 5);
		verifyFile(subdir, "big-3.avro", 3, 5);

		verifyFile(subsubdir, "skipped-4.avro", 4, 25);
		verifyFile(subsubdir, "skipped-5.avro", 5, 25);

		/*
		 * Verify the crush output.
		 */
		verifyCrushOutput(out, new int[] { 0, 1 }, new int[] { 1, 2}, new int[] { 2, 5 }, new int[] { 3, 5 });
	}

	@Test
	public void noFiles() throws Exception {
		File in = tmp.newFolder("in");

		File out = new File(tmp.getRoot(), "out");

		ToolRunner.run(job, new Crush(), new String[] {
				in.getAbsolutePath(), out.getAbsolutePath()
		});

		assertThat(out.exists(), is(false));
	}

	private void verifyCrushOutput(File crushOutput, int[]... keyCounts) throws IOException {

		List<String> actual = new ArrayList<String>();

        File file = new File(crushOutput.getAbsolutePath());

        GenericRecord pair = null;

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

		while (dataFileReader.hasNext()) {
            pair = dataFileReader.next(pair);
			actual.add(format("%s\t%d", pair.get("key"), pair.get("value")));
		}

        dataFileReader.close();

		int expLines = 0;
		List<List<String>> expected = new ArrayList<List<String>>();


		for (int[] keyCount : keyCounts) {
			int key  = keyCount[0];
			int count = keyCount[1];

			List<String> lines = new ArrayList<String>();
			expected.add(lines);

			for (int i = 0, j = 0; i < count; i++, j = j == 9 ? 0 : j + 1) {
				String line = format("%d\t%d", key, j);
				lines.add(line);
			}

			expLines += count;
		}

		/*
		 * Make sure each file's data is contiguous in the crush output file.
		 */
		for (List<String> list : expected) {
			int idx = actual.indexOf(list.get(0));

			assertThat(idx, greaterThanOrEqualTo(0));

			assertThat(actual.subList(idx, idx + list.size()), equalTo(list));
		}

		assertThat(actual.size(), equalTo(expLines));
	}

	private void createFile(File dir, String fileName, int key, int count) throws IOException {



		File file = new File(dir, fileName);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);


        GenericRecord pair = new GenericData.Record(schema);

        for (int i = 0, j = 0; i < count; i++, j = j == 9 ? 0 : j + 1) {
            pair.put("key",Integer.toString(key));
            pair.put("value",j);
            dataFileWriter.append(pair);
        }

        dataFileWriter.close();
	}

	private void verifyFile(File dir, String fileName, int key, int count) throws IOException {
		File file = new File(dir, fileName);

//		Reader reader = new Reader(FileSystem.get(job), new Path(file.getAbsolutePath()), job);

        GenericRecord pair = null;

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

		int i = 0;
		int actual = 0;

		while (dataFileReader.hasNext()) {
            pair = dataFileReader.next(pair);
			assertThat(pair.get("key").toString(), equalTo(Integer.toString(key)));
			assertThat(Integer.parseInt(pair.get("value").toString()), equalTo(i));

			if (i == 9) {
				i = 0;
			} else {
				i++;
			}

			actual++;
		}

		dataFileReader.close();

		assertThat(actual, equalTo(count));
	}

    private String createSchemaFile(File dir, String fileName) throws FileNotFoundException {
        File file = new File(dir,fileName);

        PrintWriter out = new PrintWriter(file);

        out.println(schema);

        out.close();

        return file.getAbsolutePath();
    }
}
