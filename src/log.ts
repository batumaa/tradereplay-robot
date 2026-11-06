/** Removes the key pair from anything about to be logged. */
export function mask(text: string, secrets: Array<string | undefined>): string {
  let out = text;
  for (const s of secrets) if (s) out = out.split(s).join("***");
  return out;
}
