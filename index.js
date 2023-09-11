import dotenv from 'dotenv';
import fs from 'fs';
import { GetObjectCommand, S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';

dotenv.config();

const s3Client = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});

const downloadFile = async (bucket, key) => {
    if (fs.existsSync(`./data/${key}`)) {
        console.log('Skipping existing file', key);
        return;
    }

    const params = { Bucket: bucket, Key: key };
    const command = new GetObjectCommand(params);

    try {
        const response = await s3Client.send(command);
        const data = await response.Body.transformToString();
        console.log('Writing file', key);
        fs.writeFileSync(`./data/${key}`, data);
    } catch (err) {
        console.error(`Error downloading ${key}:`, err);
    }
};

const downloadAllJsonFiles = async () => {
    const bucket = process.env.AWS_BUCKET;
    let isTruncated = true;
    let nextContinuationToken;

    while (isTruncated) {
        const listParams = {
            Bucket: bucket,
            ContinuationToken: nextContinuationToken,
        };
        const listCommand = new ListObjectsV2Command(listParams);

        try {
            // eslint-disable-next-line no-await-in-loop
            const { Contents, NextContinuationToken } = await s3Client.send(listCommand);

            if (!Contents) {
                console.log('No files found in bucket.');
                return;
            }

            // Filter JSON files
            const jsonFiles = Contents.filter(({ Key }) => Key.endsWith('.json'));

            // eslint-disable-next-line no-restricted-syntax
            for (const { Key } of jsonFiles) {
                if (Key) {
                    console.log(`Downloading ${Key}`);
                    // eslint-disable-next-line no-await-in-loop
                    await downloadFile(bucket, Key);
                    console.log(`${Key} downloaded successfully`);
                }
            }

            nextContinuationToken = NextContinuationToken;
            isTruncated = !!NextContinuationToken;
        } catch (err) {
            console.error('An error occurred:', err);
            return;
        }
    }
};

downloadAllJsonFiles()
    .then(() => console.log('All JSON files downloaded successfully'))
    .catch((err) => console.error('An error occurred:', err));
