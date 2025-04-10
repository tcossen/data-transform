import axios from "axios";
import * as fs from "fs";
import * as path from "path";
import { parseStringPromise } from "xml2js";

export async function listFiles(bucketUrl: string): Promise<string[]> {
  const response = await axios.get(bucketUrl);
  const parsedObject = await parseStringPromise(response.data);

  return parsedObject.ListBucketResult.Contents.map((item: any) => item.Key[0]);
}

async function downloadFile(
  bucketUrl: string,
  fileKey: string,
  destDirectoryPath: string
) {
  const response = await axios.get(
    `${bucketUrl}${encodeURIComponent(fileKey)}`,
    { responseType: "stream" }
  );

  const writer = fs.createWriteStream(
    path.resolve(path.join(destDirectoryPath, path.basename(fileKey)))
  );
  response.data.pipe(writer);

  return new Promise<void>((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
}

export async function downloadFiles(
  bucketUrl: string,
  fileKeys: string[],
  destDirectoryPath: string
) {
  for (const fileKey of fileKeys) {
    try {
      console.log(`Downloading file ${fileKey}`);
      await downloadFile(bucketUrl, fileKey, destDirectoryPath);
    } catch (err) {
      console.error(`Failed to download file ${fileKey}`, err);
    }
  }
}

export async function downloadAllFiles(
  bucketUrl: string,
  destDirectoryPath: string
) {
  const fileKeys = await listFiles(bucketUrl);
  if (fileKeys) {
    await downloadFiles(bucketUrl, fileKeys, destDirectoryPath);
  }
}
