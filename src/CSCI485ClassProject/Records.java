package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;

/**
 * DataRecordManager manages the data stored by the certain table. It provides interfaces to
 * get/create/update/delete certain data record(s) in a table
 */
public interface Records {

  /**
   * Insert a new row into the table.
   *
   * Given primary keys and values must match the specification in TableMetadata.
   *
   * If given attributes does not exist in the schema, attributes should be added to the schema.
   *
   * @param tableName the target tableName
   * @param primaryKeys primary keys
   * @param primaryKeysValues corresponding values of primary keys
   * @param attrNames attribute names(doesn't contain primary keys)
   * @param attrValues attribute values(doesn't contain primary key values)
   * @return StatusCode
   */
  StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues);

  /**
   * Open a cursor that iterates a table with given mode.
   *
   * Order of the records follow the primary key values' descending order.
   *
   * @param tableName the target table's name
   * @param mode the mode of cursor: READ/READ_WRITE
   * @return the cursor
   */
  Cursor openCursor(String tableName, Cursor.Mode mode);

  /**
   * Open a cursor that iterates a table with a certain predicate.
   *
   * Order of the records follow the primary key values' descending order.
   *
   * @param tableName the target tableName
   * @param attrName the target attribute Name
   * @param attrValue the attribute value for the predicate
   * @param operator the operator used by the predicate
   * @param mode the mode of cursor: READ/READ_WRITE
   * @param isUsingIndex used in Part3
   *    for READ cursor, true indicates the search should use the index on the given attribute.
   * @return Cursor
   */
  Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex);

  /**
   * Seek the cursor to the first qualified record.
   *
   * Once the cursor is initialized by getFirst, it can ONLY iterate records using getNext.
   * @param cursor the target cursor
   * @return the first qualified record
   */
  Record getFirst(Cursor cursor);

  /**
   * Seek the cursor to the last qualified record.
   *
   * Once the cursor is initialized by getLast, it can ONLY iterate records using getPrevious
   * @param cursor the target cursor
   * @return the last qualified record
   */
  Record getLast(Cursor cursor);

  /**
   * Move the cursor to the next valid record and return. If it is already at the last record, return the EOF
   *
   * getNext can only be called with cursor initialized by getFirst.
   * @param cursor the cursor to move next
   * @return Record if the next record exists, otherwise return null
   */
  Record getNext(Cursor cursor);

  /**
   * Move the cursor to the previous valid record and return. If it is already at the first record, return the EOF
   *
   * getPrevious can only be called with cursor initialized by getLast
   * @param cursor the cursor to move previous
   * @return Record if the previous record exists, otherwise return null
   */
  Record getPrevious(Cursor cursor);

  /**
   * Update the record that the cursor is pointing at, with new attribute values. Cursor must be in READ_WRITE mode.
   *
   * If the given attribute(s) do not exist in the record, the update should fail and return CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND.
   * Part3: If index structures are built on some attributes, they should also be updated
   * @param cursor the target cursor
   * @param attrNames the attribute names
   * @param attrValues the corresponding attribute values
   * @return StatusCode
   */
  StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues);

  /**
   * Delete the record that the cursor is pointing at.
   *
   * Part3: If index type is specified when opening the cursor, the corresponding index record should also be deleted
   * @param cursor the target cursor
   * @return StatusCode
   */
  StatusCode deleteRecord(Cursor cursor);


  /**
   * Commit changes made by the cursor to FDB.
   * @param cursor the target cursor
   * @return StatusCode
   */
  StatusCode commitCursor(Cursor cursor);

  /**
   * Abort the given cursor. Any changes made by the cursor will be aborted
   * @param cursor the target cursor
   * @return StatusCode
   */
  StatusCode abortCursor(Cursor cursor);

  /**
   * Delete the record with given attribute names and values in a table.
   *
   * Part3: If an index exists on the attribute referenced by updateRecord, the index should also be updated
   *
   * @param tableName the target table name
   * @param attrNames the attribute names
   * @param attrValues the corresponding attribute values
   * @return StatusCode
   */
  StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues);

  public void closeDatabase();

}
