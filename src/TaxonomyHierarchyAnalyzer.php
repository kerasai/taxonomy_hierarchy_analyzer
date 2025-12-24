<?php

namespace Kerasai\TaxonomyHierarchyAnalyzer;

use Drupal\Core\Database\Connection;
use Drupal\Core\Entity\EntityFieldManagerInterface;

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
   */
  public function __construct(
    protected readonly Connection $database,
    protected readonly EntityFieldManagerInterface $entityFieldManager,
  ) {
    // No op.
  }

  /**
   * Creates a TaxonomyHierarchyAnalyzer.
   *
   * @return static
   */
  public static function create() {
    return new self(\Drupal::database(), \Drupal::service('entity_field.manager'));
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

}
