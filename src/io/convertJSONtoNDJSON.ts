/**
 * convertJSONtoNDJSON.ts
 *
 * Convierte:
 *   [ {…}, {…}, {…} ]
 *
 * a:
 *   {…}
 *   {…}
 *   {…}
 *
 *  Streaming puro (0 RAM blow)
 *  Sin JSON.parse (solo bytes)
 */

import fs from 'fs';

const CHUNK_SIZE = 64 * 1024 * 1024; // 64MB

// Bytes importantes
const B_OPEN_BRACE = 123; // {
const B_CLOSE_BRACE = 125; // }
const B_OPEN_BRACKET = 91; // [
const B_CLOSE_BRACKET = 93; // ]
const B_QUOTE = 34; // "
const B_BACKSLASH = 92; // \

export function convertJSONtoNDJSON(inputPath: string, outputPath: string): void {
  const fd = fs.openSync(inputPath, 'r');
  const fileSize = fs.fstatSync(fd).size;

  const writeStream = fs.createWriteStream(outputPath);

  let bytesRead = 0;
  let carry = Buffer.alloc(0);

  const chunkBuf = Buffer.allocUnsafe(CHUNK_SIZE);

  let total = 0;

  while (bytesRead < fileSize) {
    const toRead = Math.min(CHUNK_SIZE, fileSize - bytesRead);

    fs.readSync(fd, chunkBuf, 0, toRead, bytesRead);
    bytesRead += toRead;

    const combined = carry.length > 0 ? Buffer.concat([carry, chunkBuf.subarray(0, toRead)]) : Buffer.from(chunkBuf.subarray(0, toRead));

    let pos = 0;
    const len = combined.length;

    // Buscar primer {
    while (pos < len && combined[pos] !== B_OPEN_BRACE) {
      pos++;
    }

    while (pos < len) {
      if (combined[pos] !== B_OPEN_BRACE) {
        pos++;
        continue;
      }

      const start = pos;
      let depth = 0;
      let inStr = false;
      let complete = false;

      while (pos < len) {
        const b = combined[pos];

        if (inStr) {
          if (b === B_BACKSLASH) {
            pos += 2;
            continue;
          }

          if (b === B_QUOTE) {
            inStr = false;
          }

          pos++;
          continue;
        }

        switch (b) {
          case B_QUOTE:
            inStr = true;
            pos++;
            break;

          case B_OPEN_BRACE:
          case B_OPEN_BRACKET:
            depth++;
            pos++;
            break;

          case B_CLOSE_BRACE:
          case B_CLOSE_BRACKET:
            depth--;
            pos++;

            if (depth === 0) {
              const record = combined.subarray(start, pos);

              writeStream.write(record);
              writeStream.write('\n');

              total++;

              complete = true;
            }
            break;

          default:
            pos++;
        }

        if (complete) {
          break;
        }
      }

      if (!complete) {
        break;
      }
    }

    // Guardar sobrante
    carry = combined.subarray(pos);

    process.stdout.write(`\r📦 ${(bytesRead / 1e9).toFixed(2)} / ${(fileSize / 1e9).toFixed(2)} GB | Registros: ${total.toLocaleString()}`);
  }

  fs.closeSync(fd);
  writeStream.end();
}
