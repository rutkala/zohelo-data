export const SQL_RELATION_DRAG_MIME = "application/x-zohelo-sql-relation";
export const SQL_INSERT_EVENT = "zohelo:insert-sql";

export interface SqlInsertEventDetail {
  sql: string;
  tabId: string;
}

export function dispatchSqlInsert(detail: SqlInsertEventDetail): void {
  document.dispatchEvent(new CustomEvent<SqlInsertEventDetail>(SQL_INSERT_EVENT, { detail }));
}

export function queueSqlInsert(detail: SqlInsertEventDetail): void {
  // Menus and the mobile explorer restore focus as they close. Insert only
  // after those two render frames so the editor can keep the final focus.
  requestAnimationFrame(() => requestAnimationFrame(() => dispatchSqlInsert(detail)));
}

export function setSqlRelationDragData(dataTransfer: DataTransfer, relation: string): void {
  dataTransfer.effectAllowed = "copy";
  dataTransfer.setData(SQL_RELATION_DRAG_MIME, relation);
  dataTransfer.setData("text/plain", relation);
}

export function selectAllFromRelation(relation: string): string {
  return `SELECT * FROM ${relation} LIMIT 100;`;
}
