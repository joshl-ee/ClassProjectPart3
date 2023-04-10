package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.IndexType;

public interface Indexes {
  /**
   * Create index structures for an existing attribute in a table.
   *
   * The table and the target attribute should exist.
   * @param tableName the target table name
   * @param indexType the index type
   * @param attrName the target attribute name
   * @return StatusCode
   */
  StatusCode createIndex(String tableName, String attrName, IndexType indexType);

  /**
   * Drop the index structure from an attribute in a table.
   *
   * The table, attribute and index structure should exist.
   * @param tableName the target table name
   * @param attrName the target attribute name
   * @return StatusCode
   */
  StatusCode dropIndex(String tableName, String attrName);

  public void closeDatabase();
}
