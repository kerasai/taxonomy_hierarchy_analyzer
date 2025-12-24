<?php

namespace Kerasai\TaxonomyHierarchyAnalyzer;

use Drupal\Core\Database\Connection;
use Drupal\Core\Entity\EntityFieldManagerInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\taxonomy\TermInterface;

/**
 * Taxonomy hierarchy analysis utility.
 */
class TaxonomyHierarchyAnalyzer {

  /**
   * Constructs a TaxonomyHierarchyAnalyzer.
   *
   * @param \Drupal\Core\Database\Connection $database
   *   The database.
   * @param \Drupal\Core\Entity\EntityFieldManagerInterface $entityFieldManager
   *   Entity field manager.
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   *   Entity type manager.
   */
  public function __construct(
    protected readonly Connection $database,
    protected readonly EntityFieldManagerInterface $entityFieldManager,
    protected readonly EntityTypeManagerInterface $entityTypeManager,
  ) {
    // No op.
  }

  /**
   * Creates a TaxonomyHierarchyAnalyzer.
   *
   * @return static
   */
  public static function create() {
    return new self(\Drupal::database(), \Drupal::service('entity_field.manager'), \Drupal::entityTypeManager());
  }

  /**
   * Count descendants of the given term.
   *
   * @param int|null $tid
   *   Parent term ID, or NULL for entire vocabulary.
   * @param string|null $vid
   *   Vocabulary ID (required when $tid is NULL).
   *
   * @return int
   *   The count of descendant terms.
   */
  public function countDescendants(?int $tid, ?string $vid = NULL): int {
    // Case where we're counting all terms, flat-out query the entire table.
    if ($tid === NULL) {
      if ($vid === NULL) {
        throw new \InvalidArgumentException('Vocabulary ID required when term ID is NULL');
      }

      return (int) $this->database->select('taxonomy_term_field_data', 't')
        ->condition('t.vid', $vid)
        ->countQuery()
        ->execute()
        ->fetchField();
    }

    $query = <<<SQL
    WITH RECURSIVE descendants AS (
      SELECT :tid AS tid

      UNION ALL

      SELECT ttp.entity_id
      FROM {taxonomy_term__parent} ttp
      INNER JOIN descendants d ON ttp.parent_target_id = d.tid
    )
    SELECT COUNT(*) - 1 FROM descendants
  SQL;

    return (int) $this->database->query($query, [':tid' => $tid])->fetchField();
  }

  /**
   * Get descendant details.
   *
   * @param int|null $tid
   *   Parent term ID, or NULL for entire vocabulary.
   * @param string|null $vid
   *   Vocabulary ID (required when $tid is NULL).
   *
   * @return array
   *   Array of objects with all taxonomy_term_field_data columns plus 'parent'.
   */
  public function getDescendants(?int $tid, ?string $vid = NULL): array {
    if ($tid === NULL) {
      if ($vid === NULL) {
        throw new \InvalidArgumentException('Vocabulary ID required when term ID is NULL');
      }

      $query = <<<SQL
      WITH RECURSIVE tree AS (
        SELECT t.tid, 0 AS depth
        FROM {taxonomy_term_field_data} t
        INNER JOIN {taxonomy_term__parent} p ON p.entity_id = t.tid
        WHERE t.vid = :vid AND p.parent_target_id = 0

        UNION ALL

        SELECT t.tid, tree.depth + 1
        FROM {taxonomy_term_field_data} t
        INNER JOIN {taxonomy_term__parent} p ON p.entity_id = t.tid
        INNER JOIN tree ON p.parent_target_id = tree.tid
        WHERE t.vid = :vid
      )
      SELECT t.*, p.parent_target_id AS parent, tree.depth
      FROM tree
      INNER JOIN {taxonomy_term_field_data} t ON t.tid = tree.tid
      LEFT JOIN {taxonomy_term__parent} p ON p.entity_id = t.tid
    SQL;

      return $this->database->query($query, [':vid' => $vid])->fetchAll();
    }

    $query = <<<SQL
    WITH RECURSIVE descendants AS (
      SELECT :tid AS tid, 0 AS depth

      UNION ALL

      SELECT ttp.entity_id, d.depth + 1
      FROM {taxonomy_term__parent} ttp
      INNER JOIN descendants d ON ttp.parent_target_id = d.tid
    )
    SELECT t.*, p.parent_target_id AS parent, d.depth
    FROM descendants d
    INNER JOIN {taxonomy_term_field_data} t ON t.tid = d.tid
    LEFT JOIN {taxonomy_term__parent} p ON p.entity_id = t.tid
    WHERE d.depth > 0
  SQL;

    return $this->database->query($query, [':tid' => $tid])->fetchAll();
  }

  /**
   * Count entities referencing a term or its descendants.
   *
   * @param \Drupal\taxonomy\TermInterface $term
   *   The term.
   * @param bool $descendants_only
   *   Only count entities referencing descendant terms. Optional, defaults to
   *   FALSE where entities referencing the original term are included.
   *
   * @return int
   *   Count of entities referencing a term or its descendants.
   */
  public function countReferencingEntities(TermInterface $term, bool $descendants_only = FALSE): int {
    $fields = $this->getTaxonomyReferenceFields($term->bundle());
    if (empty($fields)) {
      return 0;
    }

    $unions = array_map(function ($field) {
      return sprintf("SELECT '%s' AS entity_type, f.entity_id FROM {%s} f INNER JOIN descendants d ON f.%s = d.tid", $field['entity_type'], $field['table'], $field['column']);
    }, $fields);

    if ($descendants_only) {
      $anchor = "SELECT entity_id AS tid FROM {taxonomy_term__parent} WHERE parent_target_id = :tid";
    }
    else {
      $anchor = "SELECT CAST(:tid AS UNSIGNED) AS tid";
    }

    $query = sprintf(
      "WITH RECURSIVE descendants AS (
      %s

      UNION ALL

      SELECT ttp.entity_id
      FROM {taxonomy_term__parent} ttp
      INNER JOIN descendants d ON ttp.parent_target_id = d.tid
    ),
    refs AS (
      %s
    )
    SELECT COUNT(*) FROM (
      SELECT DISTINCT entity_type, entity_id FROM refs
    ) unique_refs",
      $anchor,
      implode("\nUNION ALL\n", $unions)
    );

    return (int) $this->database->query($query, [':tid' => $term->id()])->fetchField();
  }

  /**
   * Get data for entities referencing a term or its descendants.
   *
   * @param \Drupal\taxonomy\TermInterface $term
   *   The term.
   * @param bool $descendants_only
   *   Get only entities referencing descendant terms. Optional, defaults to
   *   FALSE where entities referencing the original term are included.
   *
   * @return array
   *   Entity data for entities referencing the term or its descendants.
   */
  public function getReferencingEntities(TermInterface $term, bool $descendants_only = FALSE): array {
    $fields = $this->getTaxonomyReferenceFields($term->bundle());
    if (empty($fields)) {
      return [];
    }

    $unions = array_map(function ($field) {
      return sprintf("SELECT '%s' AS entity_type, f.entity_id FROM {%s} f INNER JOIN descendants d ON f.%s = d.tid", $field['entity_type'], $field['table'], $field['column']);
    }, $fields);

    $entity_types = array_unique(array_column($fields, 'entity_type'));
    $entity_joins = [];
    $label_columns = $bundle_columns = [];

    foreach ($entity_types as $entity_type) {
      $table_info = $this->getEntityTableInfo($entity_type);
      if ($table_info) {
        $alias = $table_info['alias'];
        $entity_joins[] = sprintf(
          "LEFT JOIN {%s} %s ON r.entity_type = '%s' AND r.entity_id = %s.%s",
          $table_info['table'],
          $alias,
          $entity_type,
          $alias,
          $table_info['id_column']
        );
        $label_columns[] = "{$alias}.{$table_info['label_column']}";
        if ($table_info['bundle_column']) {
          $bundle_columns[] = "{$alias}.{$table_info['bundle_column']}";
        }
      }
    }

    $label_coalesce = 'COALESCE(' . implode(', ', $label_columns) . ') AS label';
    $bundle_coalesce = !empty($bundle_columns)
      ? 'COALESCE(' . implode(', ', $bundle_columns) . ') AS bundle'
      : "'' AS bundle";

    if ($descendants_only) {
      $anchor = "SELECT entity_id AS tid FROM {taxonomy_term__parent} WHERE parent_target_id = :tid";
    }
    else {
      $anchor = "SELECT CAST(:tid AS UNSIGNED) AS tid";
    }

    $query = sprintf(
      "WITH RECURSIVE descendants AS (
        %s

        UNION ALL

        SELECT ttp.entity_id
        FROM {taxonomy_term__parent} ttp
        INNER JOIN descendants d ON ttp.parent_target_id = d.tid
      ),
      refs AS (
        %s
      )
      SELECT DISTINCT
        r.entity_type,
        r.entity_id,
        %s,
        %s
      FROM refs r
      %s",
      $anchor,
      implode("\nUNION ALL\n", $unions),
      $bundle_coalesce,
      $label_coalesce,
      implode("\n", $entity_joins)
    );

    return $this->database->query($query, [':tid' => $term->id()])->fetchAll();
  }

  /**
   * Discover entity reference fields targeting a vocabulary.
   *
   * @param string $vid
   *   Vocabulary ID.
   * @param bool $include_parent_field
   *   Include taxonomy_term parent field. Optional, defaults to FALSE.
   *
   * @return array
   *   Array of field info with keys: entity_type, field_name, table, column.
   */
  public function getTaxonomyReferenceFields(string $vid, bool $include_parent_field = FALSE): array {
    $fields = [];
    $field_map = $this->entityFieldManager->getFieldMapByFieldType('entity_reference');

    foreach ($field_map as $entity_type_id => $entity_field_info) {
      foreach ($entity_field_info as $field_name => $field_info) {
        $table = "{$entity_type_id}__{$field_name}";

        // Short circuit if we've already determined that we have this table.
        if (!empty($fields[$table])) {
          continue;
        }

        foreach ($field_info['bundles'] as $bundle) {
          $definition = $this->entityFieldManager
            ->getFieldDefinitions($entity_type_id, $bundle)[$field_name] ?? NULL;

          if (!$definition) {
            continue;
          }

          $settings = $definition->getSettings();

          if (($settings['target_type'] ?? '') !== 'taxonomy_term') {
            continue;
          }

          // Exclude if we're configured to target specific bundles, but not
          // our bundle (vocabulary).
          $target_bundles = $settings['handler_settings']['target_bundles'] ?? [];
          if ($target_bundles && !in_array($vid, $target_bundles)) {
            continue;
          }

          $fields[$table] = [
            'entity_type' => $entity_type_id,
            'field_name' => $field_name,
            'table' => $table,
            'column' => "{$field_name}_target_id",
          ];
        }
      }
    }

    if (!$include_parent_field && !empty($fields['taxonomy_term__parent'])) {
      unset($fields['taxonomy_term__parent']);
    }

    return $fields;
  }

  /**
   * Get entity table info.
   *
   * @param string $entity_type_id
   *   The entity type ID.
   *
   * @return array|null
   *   Array with keys: table, id_column, label_column, alias.
   *   NULL if entity type has no label or data table.
   */
  protected function getEntityTableInfo(string $entity_type_id): ?array {
    try {
      $definition = $this->entityTypeManager->getDefinition($entity_type_id);
    }
    catch (\Exception $e) {
      return NULL;
    }

    if (!$data_table = $definition->getDataTable() ?? $definition->getBaseTable()) {
      return NULL;
    }

    if (!$label_key = $definition->getKey('label')) {
      return NULL;
    }

    if (!$id_key = $definition->getKey('id')) {
      return NULL;
    }

    // Generate short alias from entity type.
    $alias = substr(preg_replace('/[^a-z]/', '', $entity_type_id), 0, 3) . 'fd';

    return [
      'table' => $data_table,
      'id_column' => $id_key,
      'label_column' => $label_key,
      'bundle_column' => $definition->getKey('bundle') ?: NULL,
      'alias' => $alias,
    ];
  }

}
