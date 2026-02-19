/**
 * Query type definitions
 */

/** Query definition */
export interface Query {
  id: string;
  name: string;
  description?: string;
  /** Cypher query string */
  query: string;
}

/** Query category with nested structure */
export interface QueryCategory {
  id: string;
  name: string;
  icon?: string;
  queries?: Query[];
  subcategories?: QueryCategory[];
  expanded?: boolean;
}
