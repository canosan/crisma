import { Promise } from "es6-promise";
import { createPool } from "mysql2/promise";

const connection = createPool({
  host: "",
  port: "",
  user: "",
  password: "",
  database: "",
});

async function query(sql, params = []) {
  return await connection
    .query(sql, params)
    .then(([rows]) => rows)
    .catch(error => {
      console.error("MariaDb query error:", error);
      throw error; // Re-throw for proper error handling
    });
}

// Model Information Functions
async function allFields(table) {
  return await query(`DESCRIBE ${table}`);
}

async function infoColumns(table) {
  return await query(`SHOW COLUMNS FROM ${table}`);
}

async function infoColumn(table, column) {
  return await query(`SHOW COLUMNS FROM ${table} WHERE FIELD = ?`, [
    column.toUpperCase(),
  ]);
}

// async function primaryKey(table) {
//   return await query(`SHOW KEYS FROM ${table} WHERE Key_name = 'PRIMARY'`);
// }

// There may be tables whose primary keys are foreign keys too.
// If all primary keys are needed, uncomment and use the code above.
// This method will return only the primary keys that are not also foreign keys.

async function primaryKey(table) {
  try {
    const primaryKeys = await query(`SHOW KEYS FROM ${table} WHERE Key_name = 'PRIMARY'`);
    if (primaryKeys.length === 0) {
      return [];
    }
    const foreignKeys = await foreignKey(table);
    const foreignKeyColumns = foreignKeys.map(fk => fk.COLUMN_NAME);

    const primaryKeysNotForeign = primaryKeys.filter(pk => !foreignKeyColumns.includes(pk.Column_name));

    return primaryKeysNotForeign;
  } catch (error) {
    console.error('Error retrieving primary keys without foreign keys:', error);
    throw error;
  }
}



async function foreignKey(table) {
  return await query(
    `
    SELECT
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME,
    ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = ?
    AND (REFERENCED_TABLE_NAME != 'null' || REFERENCED_TABLE_NAME != null)
    GROUP BY REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
  `,
    [table]
  );
}

async function isForeignKey(table, column) {
  return await query(
    `
    SELECT
      REFERENCED_TABLE_NAME,
      REFERENCED_COLUMN_NAME,
      ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = ? AND REFERENCED_COLUMN_NAME = ?
  `,
    [table, column]
  );
}

async function isNull(table, column) {
  return await query(`SHOW COLUMNS FROM ${table} WHERE FIELD = ?`, [
    column,
  ]).then(results => (results[0].Null === "YES" ? true : false));
}

async function create(table, data) {
  setTimeout(() => console.log("DATA -> ", data), 2000);

  const columns = Object.keys(data).join(", ");
  const placeholders = Object.values(data)
    .map(entry => (entry = "?"))
    .join(", ");
  const values = Object.values(data);

  const sql = `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`;
  const res = await query(sql, values);
  return res;
}

async function update(table, where, data) {
  const updates = Object.keys(data)
    .map(key => `${key} = ?`)
    .join(", ");
  const values = Object.values(data).concat(where.value);

  const sql = `UPDATE ${table} SET ${updates} WHERE ${where.key} = ?`;
  const res = await query(sql, values);
  setTimeout(
    () =>
      console.log(
        "SQL -> ",
        sql,
        "| UPDATES -> ",
        updates,
        "| VALUES -> ",
        values,
        "| WHERE -> ",
        where,
        "| RESPUESTA -> ",
        res
      ),
    2000
  );
  return res;
}

async function findById(table, id) {
  // setTimeout(() => console.log('-----------------> idFilterById: ', id), 1500);
  return await query(`SELECT * FROM ${table} WHERE ${id.key} = ?`, [id.value]);
}

function objectToStringFilter(obj) {
  const filterParts = [];
  for (const [key, value] of Object.entries(obj)) {
    filterParts.push(`${key}='${value}'`);
  }
  return filterParts.join(" and ");
}

async function findByMultiplesIds(table, ids) {
  const filterString = objectToStringFilter(ids);
  console.log("|||| ### ids", filterString);

  return await query(`SELECT * FROM ${table} WHERE ${filterString}`);
}

async function findAll(table, options = {}) {
  const { where = {}, includes = [], range = null, orderBy = null } = options;

  let joinClause = "";
  let whereClause = "";
  const values = [];

  const fieldConditions = [];
  const dateStatusConditions = [];
  for (const [key, value] of Object.entries(where)) {
    if (key === "OR") {
      if (Array.isArray(value)) {
        fieldConditions.push(await Promise.all(value.map(buildCondition)));
      } else {
        throw new Error("'OR' condition must be an array.");
      }
    } else {
      dateStatusConditions.push(await buildCondition({ [key]: value }));
    }
  }

  if (fieldConditions.length > 0 || dateStatusConditions.length > 0) {
    whereClause = ` WHERE ${
      fieldConditions.length > 0 ? fieldConditions.flat().join(" OR ") : ""
    } ${
      dateStatusConditions.length > 0 && fieldConditions.length > 0 ? "AND" : ""
    } ${
      dateStatusConditions.length > 0
        ? dateStatusConditions.flat().join(" AND ")
        : ""
    }`;
  }

  async function buildCondition(condition) {
    const parts = [];

    for (const [key, value] of Object.entries(condition)) {
      if (key === "disabledAt") {
        if (value === null) {
          parts.push(`${table}.${key} IS NULL`);
        } else if (Object.prototype.hasOwnProperty.call(value, "not") && value.not === null) {
          parts.push(`${table}.${key} IS NOT NULL`);
        }
      } else if (
        typeof value === "object" &&
        value !== null &&
        !Object.prototype.hasOwnProperty.call(value, "contains")
      ) {
        if (key === "createdAt") {
          if (value.gte && value.lte) {
            const gte = new Date(value.gte)?.toISOString();
            const lte = new Date(value.lte)?.toISOString();
            parts.push(`${table}.${key} BETWEEN ? AND ?`);
            values.push(gte, lte);
          } else if (value.gte) {
            const gte = new Date(value.gte)?.toISOString();
            parts.push(`${table}.${key} >= ?`);
            values.push(gte);
          } else if (value.lte) {
            const lte = new Date(value.lte)?.toISOString();
            parts.push(`${table}.${key} <= ?`);
            values.push(lte);
          }
        } else {
          const pk = (await primaryKey(key))[0]?.Column_name;
          // key = Nombre tabla foranea, table = Nombre tabla, pk = Campo Primary Key tabla foranea
          joinClause += ` LEFT JOIN ${key} ON ${table}.${pk} = ${key}.${pk} `;
          parts.push(`${key}.${Object.keys(value)[0]} LIKE ?`);
          values.push(`%${Object.values(value)[0].contains}%`);
        }
      } else {
        // table = Nombre tabla, key = Nombre campo
        parts.push(`${table}.${key} LIKE ?`);
        values.push(`%${value.contains}%`);
      }
    }

    return parts.join(" AND ");
  }

  let orderClause = "";
  if (orderBy) {
    orderClause = ` ORDER BY ${Object.entries(orderBy[0]).map(
      ([key, value]) => `${key} ${value}`
    )}`;
  }

  let limitClause = "";
  // Importante asegurarse de que skip o take no lleguen como undefined, o no funcionaran las
  // consultas en los campos del detail, por ejemplo
  if (range && range.skip !== undefined && range.take !== undefined) {
    limitClause = ` LIMIT ${range.skip}, ${range.take}`;
  }

  const sql = `SELECT * FROM ${table}${joinClause}${whereClause}${orderClause}${limitClause}`;
  const results = await query(sql, values);

  // Procesar claves foráneas
  const fk = await foreignKey(table);

  // Comprobar en cada fila devuelta por la query si es foreign key, y nos traemos todos los datos
  // de la fila que le corresponde
  for (const result of results) {
    console.log("Result:", result);
    for (const entry of fk) {
      console.log("Foreign key entry:", entry);
      // Obtener datos de la tabla foránea
      const foreignResults = await findById(entry.REFERENCED_TABLE_NAME, {
        key: entry.REFERENCED_COLUMN_NAME,
        value: result[entry.COLUMN_NAME],
      });

      // console.log('entry.REFERENCED_TABLE_NAME:', entry.REFERENCED_TABLE_NAME);
      // console.log('entry.REFERENCED_COLUMN_NAME:', entry.REFERENCED_COLUMN_NAME);
      // console.log('result:', result);
      // console.log('entry.COLUMN_NAME:', entry.COLUMN_NAME);
      // console.log('Foreign results:', foreignResults);

      // Asignar los resultados a las propiedades correspondientes
      if (foreignResults && foreignResults.length > 0) {
        result[entry.REFERENCED_TABLE_NAME] = foreignResults[0];
      }
    }
  }

  return results;
}

async function deleteRecord(table, id) {
  return await query(`DELETE FROM ${table} WHERE ${id.key} = ?`, [id.value]);
}

async function countAll(table) {
  return await query(`SELECT COUNT(*) AS total FROM ${table}`);
}

export default {
  query,
  allFields,
  infoColumns,
  infoColumn,
  primaryKey,
  foreignKey,
  isForeignKey,
  isNull,
  create,
  update,
  findById,
  findByMultiplesIds,
  findAll,
  deleteRecord,
  countAll,
};
