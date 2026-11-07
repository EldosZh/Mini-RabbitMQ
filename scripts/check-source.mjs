// Read-only review of the source set proposed for Git. No index or commit changes.
import { execFileSync } from "node:child_process";
import { readFileSync, existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
const paths = execFileSync(
  "git",
  ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
  { encoding: "utf8" },
)
  .split("\0")
  .filter(Boolean);
let errors = 0;
for (const path of paths) {
  const text = readFileSync(path, "utf8");
  if (text.includes("\0")) {
    console.error("Binary file in source set:", path);
    errors++;
  }
  if (/(?:^|\n)[^\n]*[\t ]+\r?(?=\n|$)/.test(text)) {
    console.error("Trailing whitespace:", path);
    errors++;
  }
  if (
    /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{30,}|sk-proj-[A-Za-z0-9_-]{30,}/.test(
      text,
    )
  ) {
    console.error("Potential secret:", path);
    errors++;
  }
  if (path.endsWith(".md"))
    for (const match of text.matchAll(/\]\(([^)]+)\)/g)) {
      const link = match[1].split("#")[0];
      if (!link || link.includes("://") || link.startsWith("mailto:")) continue;
      if (!existsSync(resolve(dirname(path), link))) {
        console.error("Broken local link:", path, link);
        errors++;
      }
    }
}
if (errors) process.exitCode = 1;
else
  console.log(
    `PASS: ${paths.length} source files; no binary artifacts, trailing whitespace, common private-key/token patterns, or broken local Markdown links`,
  );
