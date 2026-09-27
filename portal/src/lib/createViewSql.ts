type SqlToken = {
  kind: "word" | "identifier" | "symbol";
  text: string;
  start: number;
  end: number;
};

type SingleStatement = {
  sql: string;
  tokens: SqlToken[];
};

const isWordStart = (character: string) => /[A-Za-z_]/.test(character);
const isWordPart = (character: string) => /[A-Za-z0-9_$]/.test(character);
const isKeyword = (token: SqlToken | undefined, keyword: string) =>
  token?.kind === "word" && token.text.toLowerCase() === keyword;

/**
 * Tokenize enough DuckDB SQL to distinguish one statement and identify a
 * CREATE VIEW header. Strings, quoted identifiers, dollar-quoted strings, and
 * comments stay opaque so their contents cannot be mistaken for syntax.
 */
const readSingleStatement = (sql: string): SingleStatement | null => {
  const tokens: SqlToken[] = [];
  let index = 0;
  let statementEnd: number | null = null;

  const addToken = (kind: SqlToken["kind"], start: number, end: number) => {
    if (statementEnd !== null) return false;
    tokens.push({ kind, text: sql.slice(start, end), start, end });
    return true;
  };

  while (index < sql.length) {
    const character = sql[index];
    if (/\s/.test(character)) {
      index += 1;
      continue;
    }

    if (character === "-" && sql[index + 1] === "-") {
      const newline = sql.indexOf("\n", index + 2);
      index = newline === -1 ? sql.length : newline + 1;
      continue;
    }

    if (character === "/" && sql[index + 1] === "*") {
      let depth = 1;
      index += 2;
      while (index < sql.length && depth > 0) {
        if (sql[index] === "/" && sql[index + 1] === "*") {
          depth += 1;
          index += 2;
        } else if (sql[index] === "*" && sql[index + 1] === "/") {
          depth -= 1;
          index += 2;
        } else {
          index += 1;
        }
      }
      if (depth > 0) return null;
      continue;
    }

    if (character === "'" || character === '"') {
      const quote = character;
      const start = index;
      index += 1;
      let closed = false;
      const escapedString =
        quote === "'" &&
        start > 0 &&
        /[Ee]/.test(sql[start - 1]) &&
        (start === 1 || !isWordPart(sql[start - 2]));
      while (index < sql.length) {
        if (sql[index] === quote) {
          if (sql[index + 1] === quote) {
            index += 2;
            continue;
          }
          index += 1;
          closed = true;
          break;
        }
        if (escapedString && sql[index] === "\\") {
          index += Math.min(2, sql.length - index);
        } else {
          index += 1;
        }
      }
      if (!closed || !addToken(quote === '"' ? "identifier" : "symbol", start, index)) {
        return null;
      }
      continue;
    }

    if (character === "$" && (sql[index + 1] === "$" || isWordStart(sql[index + 1] ?? ""))) {
      const tagMatch = sql.slice(index).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);
      if (tagMatch) {
        const start = index;
        const tag = tagMatch[0];
        const closing = sql.indexOf(tag, index + tag.length);
        if (closing === -1) return null;
        index = closing + tag.length;
        if (!addToken("symbol", start, index)) return null;
        continue;
      }
    }

    if (character === ";") {
      if (statementEnd !== null || tokens.length === 0) return null;
      statementEnd = index;
      index += 1;
      continue;
    }

    if (statementEnd !== null) return null;

    if (isWordStart(character)) {
      const start = index;
      index += 1;
      while (index < sql.length && isWordPart(sql[index])) index += 1;
      addToken("word", start, index);
      continue;
    }

    addToken("symbol", index, index + 1);
    index += 1;
  }

  if (tokens.length === 0) return null;
  return {
    sql: sql.slice(0, statementEnd ?? sql.length),
    tokens,
  };
};

const queryBodyStart = (statement: SingleStatement): number | null => {
  const { tokens } = statement;
  let cursor = 0;
  if (!isKeyword(tokens[cursor], "create")) return null;
  cursor += 1;

  if (isKeyword(tokens[cursor], "or") && isKeyword(tokens[cursor + 1], "replace")) {
    cursor += 2;
  }
  if (isKeyword(tokens[cursor], "temp") || isKeyword(tokens[cursor], "temporary")) {
    cursor += 1;
  }
  if (!isKeyword(tokens[cursor], "view")) return null;
  cursor += 1;

  if (
    isKeyword(tokens[cursor], "if") &&
    isKeyword(tokens[cursor + 1], "not") &&
    isKeyword(tokens[cursor + 2], "exists")
  ) {
    cursor += 3;
  }

  const isName = (token: SqlToken | undefined) =>
    token?.kind === "word" || token?.kind === "identifier";
  if (!isName(tokens[cursor])) return null;
  cursor += 1;
  while (tokens[cursor]?.text === "." && isName(tokens[cursor + 1])) cursor += 2;

  if (tokens[cursor]?.text === "(") {
    let depth = 0;
    do {
      const token = tokens[cursor];
      if (!token) return null;
      if (token.text === "(") depth += 1;
      if (token.text === ")") depth -= 1;
      cursor += 1;
    } while (depth > 0);
  }

  if (!isKeyword(tokens[cursor], "as")) return null;
  cursor += 1;
  if (!isKeyword(tokens[cursor], "select") && !isKeyword(tokens[cursor], "with")) return null;
  return tokens[cursor].start;
};

/** Return the SELECT/WITH body of one ordinary CREATE VIEW statement. */
export const extractCreateViewQuery = (sql: string): string | null => {
  const statement = readSingleStatement(sql);
  if (!statement) return null;
  const start = queryBodyStart(statement);
  return start === null ? null : statement.sql.slice(start);
};

const leadingKeyword = (sql: string): string | null => {
  let index = 0;
  while (index < sql.length) {
    if (/\s/.test(sql[index])) {
      index += 1;
      continue;
    }
    if (sql[index] === "-" && sql[index + 1] === "-") {
      const newline = sql.indexOf("\n", index + 2);
      index = newline === -1 ? sql.length : newline + 1;
      continue;
    }
    if (sql[index] === "/" && sql[index + 1] === "*") {
      let depth = 1;
      index += 2;
      while (index < sql.length && depth > 0) {
        if (sql[index] === "/" && sql[index + 1] === "*") {
          depth += 1;
          index += 2;
        } else if (sql[index] === "*" && sql[index + 1] === "/") {
          depth -= 1;
          index += 2;
        } else {
          index += 1;
        }
      }
      if (depth > 0) return null;
      continue;
    }
    if (!isWordStart(sql[index])) return null;
    const start = index;
    index += 1;
    while (index < sql.length && isWordPart(sql[index])) index += 1;
    return sql.slice(start, index).toLowerCase();
  }
  return null;
};

/**
 * Return SQL that DuckDB's SELECT-only serializer may inspect. SELECT/WITH
 * batches are safe because json_serialize_sql parses but never executes them;
 * mixed batches return a serializer error. CREATE VIEW is accepted only when
 * its single SELECT/WITH body can be isolated lexically.
 */
export const selectQueryForReferenceResolution = (sql: string): string | null => {
  const firstKeyword = leadingKeyword(sql);
  if (firstKeyword === "select" || firstKeyword === "with") return sql;

  const statement = readSingleStatement(sql);
  if (!statement) return null;
  const start = queryBodyStart(statement);
  return start === null ? null : statement.sql.slice(start);
};

const quoteIdentifier = (identifier: string) => `"${identifier.replace(/"/g, '""')}"`;

/** Build one session-local view statement in DuckDB's main schema. */
export const buildCreateViewSql = (name: string, query: string): string => {
  const normalizedName = name.trim();
  if (!normalizedName) throw new Error("Enter a view name.");

  const statement = readSingleStatement(query);
  if (
    !statement ||
    (!isKeyword(statement.tokens[0], "select") && !isKeyword(statement.tokens[0], "with"))
  ) {
    throw new Error("A view query must be one SELECT or WITH statement.");
  }

  const body = statement.sql;
  const bodyEnd = statement.tokens[statement.tokens.length - 1]?.end ?? body.length;
  const queryWithTerminator = `${body.slice(0, bodyEnd)};${body.slice(bodyEnd)}`;
  return `CREATE VIEW ${quoteIdentifier("main")}.${quoteIdentifier(normalizedName)} AS ${queryWithTerminator}`;
};
