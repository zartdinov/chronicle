import { sql } from 'bun'
import * as uuid from 'uuid'
import { DuckDBInstance } from '@duckdb/node-api'

const instance = await DuckDBInstance.create(undefined)
const connection = await instance.connect()
await connection.run('INSTALL spatial')
await connection.run('LOAD spatial')

const result = await connection.stream(`
    SELECT id, tags, ST_X(point) AS longitude, ST_Y(point) AS latitude
    FROM (
        SELECT feature_id AS id, to_json(tags) AS tags, ST_Centroid(geometry) AS point
        FROM '${process.env.DATASET_PATH!}'
        WHERE tags['addr:street'] IS NOT NULL
           OR tags['addr:housenumber'] IS NOT NULL
    )
`)

await sql`CREATE TABLE IF NOT EXISTS features (
  uuid uuid PRIMARY KEY,
  id varchar(255),
  tags jsonb,
  longitude float,
  latitude float
)`

function generateUUID(id: string) {
  return uuid.v5(id, uuid.NIL)
}

for await (const rows of result.yieldRowObjectJs()) {
  if (rows.length > 0) {
    console.log(`Processing ${rows.length} rows`)
    for (const row of rows) {
      row['uuid'] = generateUUID(row['id'] as string)
      row['tags'] = JSON.parse(row['tags'] as string)
    }
    await sql`
      INSERT INTO features ${sql(rows, 'uuid', 'id', 'tags', 'longitude', 'latitude')}
      ON CONFLICT (uuid) DO UPDATE SET id = EXCLUDED.id, tags = EXCLUDED.tags, longitude = EXCLUDED.longitude, latitude = EXCLUDED.latitude
    `
  }
}
