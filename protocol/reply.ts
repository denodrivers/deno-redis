import { BufReader } from "../vendor/https/deno.land/std/io/buf_reader.ts";
import type * as types from "./types.ts";
import { EOFError, ErrorReplyError, InvalidStateError } from "../errors.ts";
import { decoder } from "./_util.ts";

const IntegerReplyCode = ":".charCodeAt(0);
const BulkReplyCode = "$".charCodeAt(0);
const SimpleStringCode = "+".charCodeAt(0);
const ArrayReplyCode = "*".charCodeAt(0);
const ErrorReplyCode = "-".charCodeAt(0);

export interface ParseReply<T = unknown> {
  (reply: Uint8Array): T;
}

function parseIntegerReply(reply: Uint8Array): number {
  return Number.parseInt(decoder.decode(reply));
}
const decode = decoder.decode.bind(decoder);
const parseSimpleStringReply = decode;
const parseBulkReply = decode;

export function readReply(reader: BufReader): Promise<types.RedisReply>;
export function readReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
): Promise<T>;
export async function readReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
): Promise<types.RedisReply | unknown> {
  const res = await reader.peek(1);
  if (res == null) {
    throw new EOFError();
  }

  const code = res[0];
  if (code === ErrorReplyCode) {
    await tryReadErrorReply(reader);
  }

  switch (code) {
    case IntegerReplyCode:
      return readIntegerReply(reader, parseReply);
    case SimpleStringCode:
      return readSimpleStringReply(reader, parseReply);
    case BulkReplyCode:
      return readBulkReply(reader, parseReply);
    case ArrayReplyCode:
      return readArrayReply(reader, parseReply);
    default:
      throw new InvalidStateError(
        `unknown code: '${String.fromCharCode(code)}' (${code})`,
      );
  }
}

async function readIntegerReply(
  reader: BufReader,
): Promise<number>;
async function readIntegerReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
): Promise<T>;
async function readIntegerReply(
  reader: BufReader,
  parseReply = parseIntegerReply,
) {
  const line = await readLine(reader);
  if (line == null) {
    throw new InvalidStateError();
  }

  return parseReply(line.subarray(1, line.length));
}

function readBulkReply(reader: BufReader): Promise<string | null>;
function readBulkReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
): Promise<T | null>;
async function readBulkReply(
  reader: BufReader,
  parseReply?: ParseReply<unknown>,
) {
  const line = await readLine(reader);
  if (line == null) {
    throw new InvalidStateError();
  }

  if (line[0] !== BulkReplyCode) {
    tryParseErrorReply(line);
  }

  const size = parseSize(line);
  if (size < 0) {
    // nil bulk reply
    return null;
  }

  const dest = new Uint8Array(size + 2);
  await reader.readFull(dest);
  const body = dest.subarray(0, dest.length - 2); // Strip CR and LF
  if (parseReply) {
    return parseReply(body);
  } else {
    return parseBulkReply(body);
  }
}

async function readSimpleStringReply(
  reader: BufReader,
): Promise<string>;
async function readSimpleStringReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
): Promise<T>;
async function readSimpleStringReply(
  reader: BufReader,
  parseReply?: ParseReply<unknown>,
) {
  const line = await readLine(reader);
  if (line == null) {
    throw new InvalidStateError();
  }

  if (line[0] !== SimpleStringCode) {
    tryParseErrorReply(line);
  }
  const body = line.subarray(1, line.length);
  if (parseReply) {
    return parseReply(body);
  } else {
    return parseSimpleStringReply(body);
  }
}

export function readArrayReply(
  reader: BufReader,
): Promise<types.ConditionalArray | types.BulkNil>;
export function readArrayReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
): Promise<Array<T>>;
export async function readArrayReply<T>(
  reader: BufReader,
  parseReply?: ParseReply<T>,
) {
  const line = await readLine(reader);
  if (line == null) {
    throw new InvalidStateError();
  }

  const argCount = parseSize(line);
  if (argCount === -1) {
    // `-1` indicates a null array
    return null;
  }

  const array: Array<types.ConditionalArray[0] | Uint8Array | T> = [];
  for (let i = 0; i < argCount; i++) {
    array.push(await readReply(reader, parseReply));
  }
  return array;
}

export const okReply = "OK";

function tryParseErrorReply(line: Uint8Array): never {
  const code = line[0];
  if (code === ErrorReplyCode) {
    throw new ErrorReplyError(decoder.decode(line));
  }
  throw new Error(`invalid line: ${line}`);
}

async function tryReadErrorReply(reader: BufReader): Promise<never> {
  const line = await readLine(reader);
  if (line == null) {
    throw new InvalidStateError();
  }
  tryParseErrorReply(line);
}

async function readLine(reader: BufReader): Promise<Uint8Array> {
  const result = await reader.readLine();
  if (result == null) {
    throw new InvalidStateError();
  }

  const { line } = result;
  return line;
}

function parseSize(line: Uint8Array): number {
  const sizeStr = line.subarray(1, line.length);
  const size = parseInt(decoder.decode(sizeStr));
  return size;
}
