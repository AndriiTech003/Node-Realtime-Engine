import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";

const root = new URL("..", import.meta.url).pathname;
const skip = new Set(["node_modules", "dist", ".run", "test-results", "playwright-report", "results", ".github"]);
const exts = [".ts", ".tsx", ".js", ".mjs", ".cjs", ".jsx"];
const offenders = [];

const valueEnders = new Set([
  ts.SyntaxKind.Identifier,
  ts.SyntaxKind.NumericLiteral,
  ts.SyntaxKind.StringLiteral,
  ts.SyntaxKind.CloseParenToken,
  ts.SyntaxKind.CloseBracketToken,
  ts.SyntaxKind.CloseBraceToken,
  ts.SyntaxKind.ThisKeyword,
  ts.SyntaxKind.NoSubstitutionTemplateLiteral,
  ts.SyntaxKind.TemplateTail,
  ts.SyntaxKind.PlusPlusToken,
  ts.SyntaxKind.MinusMinusToken,
]);

function walk(dir) {
  for (const name of readdirSync(dir)) {
    if (skip.has(name)) continue;
    const full = join(dir, name);
    const stat = statSync(full);
    if (stat.isDirectory()) walk(full);
    else if (exts.some((e) => name.endsWith(e))) check(full);
  }
}

function check(file) {
  const text = readFileSync(file, "utf8");
  const variant = file.endsWith("x") ? ts.LanguageVariant.JSX : ts.LanguageVariant.Standard;
  const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, variant, text);
  const templateDepth = [];
  let previous = ts.SyntaxKind.Unknown;
  let kind = scanner.scan();
  while (kind !== ts.SyntaxKind.EndOfFileToken) {
    if (kind === ts.SyntaxKind.SingleLineCommentTrivia || kind === ts.SyntaxKind.MultiLineCommentTrivia) {
      const pos = scanner.getTokenStart();
      const comment = text.slice(pos, scanner.getTokenEnd());
      const line = text.slice(0, pos).split("\n").length;
      if (!(line === 1 && comment.startsWith("#!"))) offenders.push(`${relative(root, file)}:${line} ${comment.slice(0, 60)}`);
      kind = scanner.scan();
      continue;
    }
    if (kind === ts.SyntaxKind.WhitespaceTrivia || kind === ts.SyntaxKind.NewLineTrivia) {
      kind = scanner.scan();
      continue;
    }
    if ((kind === ts.SyntaxKind.SlashToken || kind === ts.SyntaxKind.SlashEqualsToken) && !valueEnders.has(previous)) {
      kind = scanner.reScanSlashToken();
    }
    if (kind === ts.SyntaxKind.TemplateHead) templateDepth.push(0);
    else if (kind === ts.SyntaxKind.OpenBraceToken && templateDepth.length > 0) templateDepth[templateDepth.length - 1]++;
    else if (kind === ts.SyntaxKind.CloseBraceToken && templateDepth.length > 0) {
      if (templateDepth[templateDepth.length - 1] === 0) {
        kind = scanner.reScanTemplateToken(false);
        if (kind === ts.SyntaxKind.TemplateTail) templateDepth.pop();
      } else {
        templateDepth[templateDepth.length - 1]--;
      }
    }
    previous = kind;
    kind = scanner.scan();
  }
}

walk(root);
if (offenders.length > 0) {
  console.error(`Source comments are not allowed (${offenders.length}):`);
  for (const o of offenders) console.error(`  ${o}`);
  process.exit(1);
}
console.log("no source comments found");
