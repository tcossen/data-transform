#!/usr/bin/env node

import { Command } from "commander";
import { downloadAllFiles } from "./s3Util";
import { Open } from "unzipper";
import * as path from "path";
import csvParser from "csv-parser";
import { z } from "zod";

const financialDataSchema = z.object({
  id: z.string().min(1, { message: "id is required" }),
  scale: z
    .string()
    .transform((val) => parseFloat(val))
    .refine((val) => !isNaN(val), {
      message: "scale must be a valid number",
    }),
});

export type FinancialData = z.infer<typeof financialDataSchema>;

type CSVData = {
  headers: string[];
  rows: Record<string, string[]>;
};

async function readCsv(
  directoryPath: string,
  fileName: string
): Promise<Record<string, string>[]> {
  const records: Record<string, string>[] = [];

  const zipContents = await Open.file(
    path.resolve(path.join(directoryPath, fileName))
  );

  for (const file of zipContents.files) {
    if (file.type === "File" && path.extname(file.path) === ".csv") {
      file
        .stream()
        .pipe(csvParser())
        .on("data", (row: Record<string, string>) => {
          // console.log(row);
          records.push(row);
        })
        .on("end", () => {
          console.log(`Finished processing ${file.path}`);
        })
        .on("error", (err: Error) => {
          console.error(`Error processing ${file.path}:`, err);
        });
    } else {
      console.warn(`Unexpected file in ${fileName}: ${file.path}`);
    }
  }

  return records;
}

function getValueByRowAndColumn(
  csvData: CSVData,
  rowName: string,
  columnName: string
): string | null {
  const dateIndex = csvData.headers.findIndex((d) => d === columnName);
  if (dateIndex === -1) return null;

  const row = csvData.rows[rowName];
  return row ? row[dateIndex] : null;
}

const program = new Command();

program
  .name("data-transform")
  .description(
    "Retrieves and transforms financial data from a public S3 bucket"
  )
  .version("1.0.0")
  .argument("<bucketUrl>", "the URL of the public S3 bucket to fetch data from")
  .option("-d, --directory <path>", "the directory that data is downloaded to")
  .action(async (bucketUrl, options) => {
    const downloadDirectory = options.directory || "./";
    console.log(
      `Fetching data from ${bucketUrl} and saving to ${downloadDirectory}`
    );
    await downloadAllFiles(bucketUrl, downloadDirectory);
    const data = await readCsv(downloadDirectory, "MNZIRS0108.zip");
    console.log(data);
  });

program.parse();

if (process.argv.length <= 2) {
  program.help();
}
