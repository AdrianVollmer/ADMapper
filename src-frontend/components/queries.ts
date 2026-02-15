/**
 * Queries Component
 *
 * Hierarchical query browser with built-in and custom queries.
 */

import { getRenderer } from "./graph-view";
import { escapeHtml } from "../utils/html";
import { executeQueryWithHistory, getQueryErrorMessage } from "../utils/query";

/** Query definition */
export interface Query {
  id: string;
  name: string;
  description?: string;
  /** CozoDB query string */
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

/** Built-in queries organized by category */
const BUILTIN_QUERIES: QueryCategory[] = [
  {
    id: "domain-recon",
    name: "Domain Recon",
    expanded: true,
    queries: [
      {
        id: "all-domain-admins",
        name: "Domain Admins",
        description: "All members of Domain Admins group",
        query: `?[user] := *MemberOf{from: user, to: group}, *Group{id: group, name: "Domain Admins"}`,
      },
      {
        id: "all-dcs",
        name: "Domain Controllers",
        description: "All domain controller computers",
        query: `?[computer] := *Computer{id: computer}, *MemberOf{from: computer, to: group}, group = "Domain Controllers"`,
      },
      {
        id: "domain-trusts",
        name: "Domain Trusts",
        description: "All domain trust relationships",
        query: `?[from, to, type] := *TrustedBy{from, to, trusttype: type}`,
      },
    ],
  },
  {
    id: "dangerous-privileges",
    name: "Dangerous Privileges",
    expanded: true,
    subcategories: [
      {
        id: "dcsync",
        name: "DCSync",
        queries: [
          {
            id: "dcsync-rights",
            name: "DCSync Rights",
            description: "Principals with DCSync privileges",
            query: `?[principal] := *DCSync{from: principal, to: domain}`,
          },
          {
            id: "dcsync-non-da",
            name: "DCSync (Non-Domain Admins)",
            description: "Non-DA principals with DCSync",
            query: `?[principal] := *DCSync{from: principal}, not *MemberOf{from: principal, to: "Domain Admins"}`,
          },
        ],
      },
      {
        id: "delegation",
        name: "Delegation",
        queries: [
          {
            id: "unconstrained",
            name: "Unconstrained Delegation",
            description: "Computers with unconstrained delegation",
            query: `?[computer] := *Computer{id: computer, unconstraineddelegation: true}`,
          },
          {
            id: "constrained",
            name: "Constrained Delegation",
            description: "Principals with constrained delegation",
            query: `?[principal, spn] := *AllowedToDelegate{from: principal, to: spn}`,
          },
          {
            id: "rbcd",
            name: "Resource-Based Constrained Delegation",
            description: "Principals that can RBCD to a target",
            query: `?[from, to] := *AllowedToAct{from, to}`,
          },
        ],
      },
      {
        id: "acl-abuse",
        name: "ACL Abuse",
        queries: [
          {
            id: "genericall",
            name: "GenericAll Rights",
            description: "Principals with GenericAll on objects",
            query: `?[from, to] := *GenericAll{from, to}`,
          },
          {
            id: "genericwrite",
            name: "GenericWrite Rights",
            description: "Principals with GenericWrite on objects",
            query: `?[from, to] := *GenericWrite{from, to}`,
          },
          {
            id: "writedacl",
            name: "WriteDacl Rights",
            description: "Principals that can modify DACLs",
            query: `?[from, to] := *WriteDacl{from, to}`,
          },
          {
            id: "writeowner",
            name: "WriteOwner Rights",
            description: "Principals that can change ownership",
            query: `?[from, to] := *WriteOwner{from, to}`,
          },
        ],
      },
    ],
  },
  {
    id: "kerberos",
    name: "Kerberos Attacks",
    queries: [
      {
        id: "kerberoastable",
        name: "Kerberoastable Users",
        description: "Users with SPNs (kerberoastable)",
        query: `?[user, spn] := *User{id: user, hasspn: true, serviceprincipalnames: spn}`,
      },
      {
        id: "asreproastable",
        name: "AS-REP Roastable",
        description: "Users without preauth required",
        query: `?[user] := *User{id: user, dontreqpreauth: true}`,
      },
    ],
  },
  {
    id: "sessions",
    name: "Sessions & Access",
    queries: [
      {
        id: "admin-sessions",
        name: "Admin Sessions",
        description: "Where domain admins are logged in",
        query: `?[admin, computer] := *HasSession{from: admin, to: computer}, *MemberOf{from: admin, to: "Domain Admins"}`,
      },
      {
        id: "rdp-users",
        name: "RDP Access",
        description: "Users with RDP access to computers",
        query: `?[user, computer] := *CanRDP{from: user, to: computer}`,
      },
      {
        id: "local-admins",
        name: "Local Admins",
        description: "Users with local admin rights",
        query: `?[user, computer] := *AdminTo{from: user, to: computer}`,
      },
    ],
  },
  {
    id: "high-value",
    name: "High Value Targets",
    queries: [
      {
        id: "high-value-nodes",
        name: "All High Value",
        description: "All nodes marked as high value",
        query: `?[node, type] := *Node{id: node, type, highvalue: true}`,
      },
    ],
  },
  {
    id: "certificates",
    name: "Certificates (ADCS)",
    queries: [
      {
        id: "vuln-templates",
        name: "Vulnerable Templates",
        description: "Certificate templates with dangerous settings",
        query: `?[template] := *CertTemplate{id: template, enrolleesuppliessubject: true}`,
      },
      {
        id: "esc1",
        name: "ESC1 - Enrollee Supplies Subject",
        description: "Templates vulnerable to ESC1",
        query: `?[template, ca] := *CertTemplate{id: template, enrolleesuppliessubject: true}, *PublishedTo{from: template, to: ca}`,
      },
    ],
  },
];

/** Custom queries loaded from file */
let customQueries: QueryCategory[] = [];

/** Current filter text */
let filterText = "";

/** DOM elements */
let queryTreeEl: HTMLElement | null = null;
let queryFilterInput: HTMLInputElement | null = null;

/** Track expanded state */
const expandedCategories = new Set<string>();

/** Initialize queries component */
export function initQueries(): void {
  queryTreeEl = document.getElementById("query-tree");
  queryFilterInput = document.getElementById("query-filter") as HTMLInputElement;

  if (queryFilterInput) {
    queryFilterInput.addEventListener("input", () => {
      filterText = queryFilterInput!.value.trim().toLowerCase();
      renderQueryTree();
    });
  }

  // Set initial expanded state
  for (const cat of BUILTIN_QUERIES) {
    if (cat.expanded) {
      expandedCategories.add(cat.id);
    }
  }

  // Load custom queries from localStorage
  loadCustomQueries();

  // Render the tree
  renderQueryTree();

  // Handle clicks on the tree
  document.addEventListener("click", handleTreeClick);
}

/** Load custom queries from localStorage */
function loadCustomQueries(): void {
  try {
    const stored = localStorage.getItem("admapper-custom-queries");
    if (stored) {
      customQueries = JSON.parse(stored);
    }
  } catch {
    customQueries = [];
  }
}

/** Save custom queries to localStorage */
export function saveCustomQueries(): void {
  localStorage.setItem("admapper-custom-queries", JSON.stringify(customQueries));
}

/** Add a custom query */
export function addCustomQuery(categoryId: string, query: Query): void {
  let category = customQueries.find((c) => c.id === categoryId);
  if (!category) {
    category = { id: categoryId, name: categoryId, queries: [] };
    customQueries.push(category);
  }
  if (!category.queries) {
    category.queries = [];
  }
  category.queries.push(query);
  saveCustomQueries();
  renderQueryTree();
}

/** Render the query tree */
function renderQueryTree(): void {
  if (!queryTreeEl) return;

  const allCategories = [...BUILTIN_QUERIES];
  if (customQueries.length > 0) {
    allCategories.push({
      id: "custom",
      name: "Custom Queries",
      subcategories: customQueries,
    });
  }

  const html = renderCategories(allCategories, 0);
  queryTreeEl.innerHTML = html || '<div class="text-sm text-gray-500 p-2">No queries match filter</div>';
}

/** Render categories recursively */
function renderCategories(categories: QueryCategory[], depth: number): string {
  let html = "";

  for (const category of categories) {
    const catHtml = renderCategory(category, depth);
    if (catHtml) {
      html += catHtml;
    }
  }

  return html;
}

/** Render a single category */
function renderCategory(category: QueryCategory, depth: number): string {
  const isExpanded = expandedCategories.has(category.id);

  // Filter queries
  const filteredQueries = (category.queries || []).filter(
    (q) => !filterText || q.name.toLowerCase().includes(filterText) || q.description?.toLowerCase().includes(filterText)
  );

  // Filter subcategories recursively
  const filteredSubcats = (category.subcategories || [])
    .map((sub) => {
      const subHtml = renderCategory(sub, depth + 1);
      return { sub, html: subHtml };
    })
    .filter((s) => s.html);

  // If nothing matches filter, skip this category
  if (filterText && filteredQueries.length === 0 && filteredSubcats.length === 0) {
    return "";
  }

  // When filtering, auto-expand categories with matches
  const shouldExpand = filterText ? true : isExpanded;

  const indent = depth * 12;
  let html = `
    <div class="query-category" data-category-id="${escapeHtml(category.id)}">
      <div
        class="query-category-header"
        style="padding-left: ${indent + 8}px"
        data-action="toggle-category"
        data-category-id="${escapeHtml(category.id)}"
      >
        <svg class="query-expand-icon ${shouldExpand ? "expanded" : ""}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M9 18l6-6-6-6"/>
        </svg>
        <span class="query-category-name">${escapeHtml(category.name)}</span>
        <span class="query-count">${countQueries(category)}</span>
      </div>
  `;

  if (shouldExpand) {
    html += `<div class="query-category-content">`;

    // Render queries
    for (const query of filteredQueries) {
      html += `
        <div
          class="query-item"
          style="padding-left: ${indent + 24}px"
          data-action="run-query"
          data-query-id="${escapeHtml(query.id)}"
          title="${escapeHtml(query.description || query.name)}"
        >
          <svg class="query-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/>
          </svg>
          <span class="query-name">${highlightMatch(query.name, filterText)}</span>
        </div>
      `;
    }

    // Render subcategories
    for (const { html: subHtml } of filteredSubcats) {
      html += subHtml;
    }

    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

/** Count total queries in a category (including subcategories) */
function countQueries(category: QueryCategory): number {
  let count = category.queries?.length || 0;
  if (category.subcategories) {
    for (const sub of category.subcategories) {
      count += countQueries(sub);
    }
  }
  return count;
}

/** Highlight matching text */
function highlightMatch(text: string, filter: string): string {
  if (!filter) return escapeHtml(text);

  const escaped = escapeHtml(text);
  const lowerText = text.toLowerCase();
  const idx = lowerText.indexOf(filter);

  if (idx === -1) return escaped;

  const before = escapeHtml(text.slice(0, idx));
  const match = escapeHtml(text.slice(idx, idx + filter.length));
  const after = escapeHtml(text.slice(idx + filter.length));

  return `${before}<mark class="query-highlight">${match}</mark>${after}`;
}

/** Handle clicks on the tree */
function handleTreeClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Toggle category
  const categoryHeader = target.closest('[data-action="toggle-category"]') as HTMLElement;
  if (categoryHeader) {
    const categoryId = categoryHeader.getAttribute("data-category-id");
    if (categoryId) {
      if (expandedCategories.has(categoryId)) {
        expandedCategories.delete(categoryId);
      } else {
        expandedCategories.add(categoryId);
      }
      renderQueryTree();
    }
    return;
  }

  // Run query
  const queryItem = target.closest('[data-action="run-query"]') as HTMLElement;
  if (queryItem) {
    const queryId = queryItem.getAttribute("data-query-id");
    if (queryId) {
      runQuery(queryId);
    }
    return;
  }
}

/** Find a query by ID */
function findQuery(queryId: string, categories: QueryCategory[] = BUILTIN_QUERIES): Query | null {
  for (const category of categories) {
    if (category.queries) {
      const query = category.queries.find((q) => q.id === queryId);
      if (query) return query;
    }
    if (category.subcategories) {
      const found = findQuery(queryId, category.subcategories);
      if (found) return found;
    }
  }
  // Also check custom queries
  if (categories === BUILTIN_QUERIES && customQueries.length > 0) {
    return findQuery(queryId, customQueries);
  }
  return null;
}

/** Run a query */
async function runQuery(queryId: string): Promise<void> {
  const query = findQuery(queryId);
  if (!query) {
    console.warn(`Query not found: ${queryId}`);
    return;
  }

  console.log(`Running query: ${query.name}`);
  console.log(`Query: ${query.query}`);

  const renderer = getRenderer();
  if (!renderer) {
    console.warn("No graph renderer available");
    return;
  }

  try {
    const result = await executeQueryWithHistory(query.name, query.query, true);

    // Show results
    if (result.graph && result.graph.nodes.length > 0) {
      // TODO: Load graph into renderer
      console.log("Query returned graph:", result.graph);
      alert(`Query "${query.name}" returned ${result.graph.nodes.length} nodes and ${result.graph.edges.length} edges`);
    } else {
      alert(`Query "${query.name}" returned ${result.resultCount} rows`);
    }
  } catch (err) {
    console.error("Query execution failed:", err);
    alert(`Query failed: ${getQueryErrorMessage(err)}`);
  }
}

/** Import queries from JSON file */
export async function importQueries(file: File): Promise<void> {
  try {
    const text = await file.text();
    const imported = JSON.parse(text) as QueryCategory[];
    customQueries.push(...imported);
    saveCustomQueries();
    renderQueryTree();
  } catch (err) {
    console.error("Failed to import queries:", err);
    throw err;
  }
}

/** Export custom queries to JSON */
export function exportQueries(): string {
  return JSON.stringify(customQueries, null, 2);
}
