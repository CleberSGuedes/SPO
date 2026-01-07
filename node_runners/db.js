const sql = require("mssql");
const path = require("path");
const dotenv = require("dotenv");

function loadEnv() {
  dotenv.config({ path: path.resolve(__dirname, "..", ".env") });
}

function getFirst(...names) {
  for (const name of names) {
    const val = process.env[name];
    if (val && String(val).trim() !== "") return String(val).trim();
  }
  return "";
}

function asBoolNo(value) {
  const v = String(value || "").trim().toLowerCase();
  return new Set(["no", "false", "0", "nao", "n"]).has(v);
}

function buildMssqlConfig() {
  const user = getFirst("DB_USER_HMG", "DB_USER");
  const password = getFirst("DB_PASSWORD_HMG", "DB_PASSWORD");
  const host = getFirst("DB_HOST_HMG", "DB_HOST");
  const port = getFirst("DB_PORT_HMG", "DB_PORT");
  const database = getFirst("DB_NAME_HMG", "DB_NAME");
  const encryptNo = asBoolNo(process.env.DB_ENCRYPT || "yes");

  return {
    user,
    password,
    server: host,
    port: Number(port || 1433),
    database,
    options: {
      encrypt: !encryptNo,
      trustServerCertificate: encryptNo,
    },
  };
}

async function connect() {
  loadEnv();
  const config = buildMssqlConfig();
  const pool = await sql.connect(config);
  return pool;
}

function mapSqlType(column) {
  const type = String(column.data_type || "").toLowerCase();
  const charLen = column.character_maximum_length;
  const precision = column.numeric_precision;
  const scale = column.numeric_scale;

  if (type === "int") return sql.Int;
  if (type === "bigint") return sql.BigInt;
  if (type === "smallint") return sql.SmallInt;
  if (type === "tinyint") return sql.TinyInt;
  if (type === "bit") return sql.Bit;
  if (type === "float") return sql.Float;
  if (type === "real") return sql.Real;
  if (type === "money") return sql.Money;
  if (type === "smallmoney") return sql.SmallMoney;
  if (type === "decimal" || type === "numeric") return sql.Numeric(Number(precision) || 18, Number(scale) || 2);
  if (type === "date") return sql.Date;
  if (type === "datetime") return sql.DateTime;
  if (type === "datetime2") return sql.DateTime2;
  if (type === "smalldatetime") return sql.SmallDateTime;
  if (type === "time") return sql.Time;

  if (["varchar", "nvarchar", "char", "nchar", "text", "ntext"].includes(type)) {
    if (!charLen || Number(charLen) < 0) return sql.NVarChar(sql.MAX);
    return sql.NVarChar(Number(charLen));
  }
  return sql.NVarChar(sql.MAX);
}

function sanitizeSqlString(value) {
  if (value === null || value === undefined) return null;
  let text = String(value);
  text = text.replace(
    /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g,
    ""
  );
  return text;
}

async function loadTableSchema(pool, tableName, columns) {
  const request = pool.request();
  const colList = columns.map((col, idx) => `@c${idx}`).join(",");
  columns.forEach((col, idx) => request.input(`c${idx}`, sql.NVarChar, col));
  const query = `
    SELECT COLUMN_NAME as column_name,
           DATA_TYPE as data_type,
           CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
           NUMERIC_PRECISION as numeric_precision,
           NUMERIC_SCALE as numeric_scale
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @tableName
      AND COLUMN_NAME IN (${colList})
  `;
  request.input("tableName", sql.NVarChar, tableName);
  const result = await request.query(query);
  const lookup = new Map();
  for (const row of result.recordset) {
    lookup.set(row.column_name, row);
  }
  const ordered = columns.map((col) => {
    const info = lookup.get(col);
    if (!info) {
      return { column_name: col, data_type: "nvarchar", character_maximum_length: -1 };
    }
    return info;
  });
  return ordered;
}

async function bulkInsert(pool, tableName, columns, rows) {
  if (!rows.length) return;
  const schema = await loadTableSchema(pool, tableName, columns);
  const table = new sql.Table(tableName);
  table.create = false;
  for (const col of schema) {
    if (col.column_name === "ativo") {
      table.columns.add(col.column_name, sql.Bit, { nullable: true });
      continue;
    }
    table.columns.add(col.column_name, mapSqlType(col), { nullable: true });
  }
  for (const row of rows) {
    const values = columns.map((col, idx) => {
      const val = col in row ? row[col] : null;
      const meta = schema[idx];
      if (!meta) return val;
      const type = String(meta.data_type || "").toLowerCase();
      if (["varchar", "nvarchar", "char", "nchar", "text", "ntext"].includes(type)) {
        const cleaned = sanitizeSqlString(val);
        if (cleaned === null) return null;
        const maxLen = Number(meta.character_maximum_length);
        if (Number.isFinite(maxLen) && maxLen > 0) {
          return cleaned.slice(0, maxLen);
        }
        return cleaned;
      }
      if (type === "bit") {
        if (val === null || val === undefined) return null;
        if (typeof val === "boolean") return val ? 1 : 0;
        if (typeof val === "number") return val ? 1 : 0;
        const text = String(val).trim().toLowerCase();
        if (text === "true" || text === "1") return 1;
        if (text === "false" || text === "0") return 0;
      }
      return val;
    });
    table.rows.add(...values);
  }
  await pool.request().bulk(table);
}

module.exports = {
  connect,
  bulkInsert,
};
