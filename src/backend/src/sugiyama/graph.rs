//! DAG construction and cycle removal for Sugiyama layout.

/// Internal graph representation for the Sugiyama pipeline.
pub(crate) struct DagGraph {
    pub n: usize,
    pub out_adj: Vec<Vec<usize>>,
    pub in_adj: Vec<Vec<usize>>,
}

impl DagGraph {
    /// Build from node count and edge list. Removes self-loops and deduplicates.
    pub fn new(n: usize, edges: &[[usize; 2]]) -> Self {
        let mut out_adj = vec![vec![]; n];
        let mut in_adj = vec![vec![]; n];
        for &[s, t] in edges {
            if s != t && !out_adj[s].contains(&t) {
                out_adj[s].push(t);
                in_adj[t].push(s);
            }
        }
        DagGraph { n, out_adj, in_adj }
    }

    /// Remove cycles by reversing back-edges found during iterative DFS.
    pub fn remove_cycles(&mut self) {
        #[derive(Clone, Copy, PartialEq)]
        enum State {
            Unvisited,
            Visiting,
            Done,
        }

        let mut state = vec![State::Unvisited; self.n];
        let mut stack: Vec<(usize, usize)> = Vec::new(); // (node, edge_index)
        let mut to_reverse: Vec<(usize, usize)> = Vec::new();

        for start in 0..self.n {
            if state[start] != State::Unvisited {
                continue;
            }
            stack.push((start, 0));
            state[start] = State::Visiting;

            while let Some(&mut (node, ref mut idx)) = stack.last_mut() {
                if *idx < self.out_adj[node].len() {
                    let target = self.out_adj[node][*idx];
                    *idx += 1;
                    match state[target] {
                        State::Unvisited => {
                            state[target] = State::Visiting;
                            stack.push((target, 0));
                        }
                        State::Visiting => {
                            to_reverse.push((node, target));
                        }
                        State::Done => {}
                    }
                } else {
                    state[node] = State::Done;
                    stack.pop();
                }
            }
        }

        for (s, t) in to_reverse {
            self.out_adj[s].retain(|&x| x != t);
            self.in_adj[t].retain(|&x| x != s);
            if !self.out_adj[t].contains(&s) {
                self.out_adj[t].push(s);
                self.in_adj[s].push(t);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_dag_no_cycles() {
        let mut g = DagGraph::new(3, &[[0, 1], [1, 2]]);
        g.remove_cycles();
        assert!(g.out_adj[0].contains(&1));
        assert!(g.out_adj[1].contains(&2));
    }

    #[test]
    fn single_back_edge() {
        let mut g = DagGraph::new(3, &[[0, 1], [1, 2], [2, 0]]);
        g.remove_cycles();
        // After cycle removal, 2->0 should be reversed to 0->2
        // The graph should be acyclic
        assert!(!has_cycle(&g));
    }

    #[test]
    fn self_loop_removed() {
        let g = DagGraph::new(2, &[[0, 0], [0, 1]]);
        assert!(!g.out_adj[0].contains(&0));
        assert!(g.out_adj[0].contains(&1));
    }

    #[test]
    fn duplicate_edges_removed() {
        let g = DagGraph::new(2, &[[0, 1], [0, 1]]);
        assert_eq!(g.out_adj[0].len(), 1);
    }

    fn has_cycle(g: &DagGraph) -> bool {
        #[derive(Clone, Copy, PartialEq)]
        enum S {
            White,
            Gray,
            Black,
        }
        let mut state = vec![S::White; g.n];
        let mut stack = Vec::new();
        for start in 0..g.n {
            if state[start] != S::White {
                continue;
            }
            stack.push((start, 0usize));
            state[start] = S::Gray;
            while let Some(&mut (node, ref mut idx)) = stack.last_mut() {
                if *idx < g.out_adj[node].len() {
                    let t = g.out_adj[node][*idx];
                    *idx += 1;
                    if state[t] == S::Gray {
                        return true;
                    }
                    if state[t] == S::White {
                        state[t] = S::Gray;
                        stack.push((t, 0));
                    }
                } else {
                    state[node] = S::Black;
                    stack.pop();
                }
            }
        }
        false
    }
}
