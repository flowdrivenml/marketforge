//! Admitted source-sized interval index, independent of event count. Ends are
//! exclusive i128 so an i64::MAX timestamp never overflows. Rank is a frozen
//! validated coverage/stable-identity order, never completion/task priority.
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, Copy)]
pub(crate) struct Segment {
    pub source: usize,
    pub start: i128,
    pub end: i128,
}
pub(crate) fn ownership(
    ranges: &[(usize, i128, i128)],
    ranks: &BTreeMap<usize, u64>,
) -> Vec<Segment> {
    let mut points = BTreeMap::<i128, Vec<(bool, usize)>>::new();
    for &(source, start, end) in ranges {
        points.entry(start).or_default().push((true, source));
        points.entry(end).or_default().push((false, source));
    }
    let mut active = BTreeSet::new();
    let mut result: Vec<Segment> = Vec::new();
    let mut previous = None;
    for (point, events) in points {
        if let Some(start) = previous
            && let Some(&(_, source)) = active.first()
        {
            if let Some(last) = result.last_mut()
                && last.source == source
                && last.end == start
            {
                last.end = point;
            } else {
                result.push(Segment {
                    source,
                    start,
                    end: point,
                });
            }
        }
        for (begin, source) in events {
            if begin {
                active.insert((ranks[&source], source));
            } else {
                active.remove(&(ranks[&source], source));
            }
        }
        previous = Some(point);
    }
    result
}
pub(crate) fn overlapping(ranges: &[(usize, i128, i128)]) -> BTreeSet<usize> {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|&(source, start, _)| (start, source));
    let mut high = i128::MIN;
    let mut result = BTreeSet::new();
    for (i, &(source, start, end)) in sorted.iter().enumerate() {
        if high > start || sorted.get(i + 1).is_some_and(|(_, next, _)| *next < end) {
            result.insert(source);
        }
        high = high.max(end);
    }
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn indexed_ownership_matches_exhaustive_oracle_for_nested_chained_equal_and_extreme_ranges() {
        for n in [1, 3, 32] {
            let ranges: Vec<_> = (0..n)
                .map(|i| (i, (i % 7) as i128, (i % 7 + 1 + (i * 13) % 11) as i128))
                .chain([(n, i64::MAX as i128, i64::MAX as i128 + 1)])
                .collect();
            for later in [false, true] {
                let mut order: Vec<_> = ranges.iter().collect();
                order.sort_by_key(|&&(i, start, _)| (if later { -start } else { start }, i));
                let ranks = order
                    .into_iter()
                    .enumerate()
                    .map(|(rank, &(i, _, _))| (i, rank as u64))
                    .collect();
                let result = ownership(&ranges, &ranks);
                for t in (0..20).chain([i64::MAX as i128]) {
                    let expected = ranges
                        .iter()
                        .filter(|&&(_, start, end)| start <= t && t < end)
                        .min_by_key(|&&(i, _, _)| ranks[&i])
                        .map(|&(i, _, _)| i);
                    assert_eq!(
                        result
                            .iter()
                            .find(|s| s.start <= t && t < s.end)
                            .map(|s| s.source),
                        expected
                    );
                }
                let expected: BTreeSet<_> = ranges
                    .iter()
                    .filter(|&&(i, a, b)| ranges.iter().any(|&(j, c, d)| i != j && a < d && c < b))
                    .map(|&(i, _, _)| i)
                    .collect();
                assert_eq!(overlapping(&ranges), expected);
            }
        }
    }
}
