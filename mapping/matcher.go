package mapping

import (
	"sort"
	"strings"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/log"
)

func (m *Mapping) pointMatcher() (NodeMatcher, error) {
	mappings := make(TagTableMapping)
	m.mappings(PointTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(PointTable, filters)
	tables, err := m.tables(PointTable)
	return &tagMatcher{
		mappings:     mappings,
		filters:      filters,
		tables:       tables,
		valueOptions: m.valueOptions(PointTable),
		matchAreas:   false,
	}, err
}

func (m *Mapping) lineStringMatcher() (WayMatcher, error) {
	mappings := make(TagTableMapping)
	m.mappings(LineStringTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(LineStringTable, filters)
	tables, err := m.tables(LineStringTable)
	return &tagMatcher{
		mappings:     mappings,
		filters:      filters,
		tables:       tables,
		valueOptions: m.valueOptions(LineStringTable),
		matchAreas:   false,
	}, err
}

func (m *Mapping) polygonMatcher() (RelWayMatcher, error) {
	mappings := make(TagTableMapping)
	m.mappings(PolygonTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(PolygonTable, filters)
	relFilters := make(tableElementFilters)
	m.addRelationFilters(PolygonTable, relFilters)
	tables, err := m.tables(PolygonTable)
	return &tagMatcher{
		mappings:     mappings,
		filters:      filters,
		tables:       tables,
		relFilters:   relFilters,
		valueOptions: m.valueOptions(PolygonTable),
		matchAreas:   true,
	}, err
}

func (m *Mapping) relationMatcher() (RelationMatcher, error) {
	mappings := make(TagTableMapping)
	m.mappings(RelationTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(PolygonTable, filters)
	m.addTypedFilters(RelationTable, filters)
	relFilters := make(tableElementFilters)
	m.addRelationFilters(RelationTable, relFilters)
	tables, err := m.tables(RelationTable)
	return &tagMatcher{
		mappings:     mappings,
		filters:      filters,
		tables:       tables,
		relFilters:   relFilters,
		valueOptions: m.valueOptions(RelationTable),
		matchAreas:   true,
	}, err
}

func (m *Mapping) relationMemberMatcher() (RelationMatcher, error) {
	mappings := make(TagTableMapping)
	m.mappings(RelationMemberTable, mappings)
	filters := make(tableElementFilters)
	m.addFilters(filters)
	m.addTypedFilters(RelationMemberTable, filters)
	relFilters := make(tableElementFilters)
	m.addRelationFilters(RelationMemberTable, relFilters)
	tables, err := m.tables(RelationMemberTable)
	return &tagMatcher{
		mappings:     mappings,
		filters:      filters,
		tables:       tables,
		relFilters:   relFilters,
		valueOptions: m.valueOptions(RelationMemberTable),
		matchAreas:   true,
	}, err
}

type NodeMatcher interface {
	MatchNode(node *osm.Node) []Match
}

type WayMatcher interface {
	MatchWay(way *osm.Way) []Match
}

type RelationMatcher interface {
	MatchRelation(rel *osm.Relation) []Match
}

type RelWayMatcher interface {
	WayMatcher
	RelationMatcher
}

type Match struct {
	Key     string
	Value   string
	Table   DestTable
	builder *rowBuilder
}

func (m *Match) Row(elem *osm.Element, geom *geom.Geometry) []interface{} {
	return m.builder.MakeRow(elem, geom, *m)
}

func (m *Match) MemberRow(rel *osm.Relation, member *osm.Member, memberIndex int, geom *geom.Geometry) []interface{} {
	return m.builder.MakeMemberRow(rel, member, memberIndex, geom, *m)
}

type tableValueOptions struct {
	splitValues bool
	multiValues map[Key]struct{}
}

type tagMatcher struct {
	mappings     TagTableMapping
	tables       map[string]*rowBuilder
	filters      tableElementFilters
	relFilters   tableElementFilters
	valueOptions map[string]tableValueOptions
	matchAreas   bool
}

const debugNodeID int64 = 2791300049

func (tm *tagMatcher) logBusStopMatches(elemType string, id int64, closed bool, relation bool, matches []Match) {
	if len(matches) == 0 {
		log.Printf("[info] no match for highway=bus_stop on %s %d (closed=%t relation=%t)", elemType, id, closed, relation)
		return
	}
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		if match.Table.SubMapping != "" {
			names = append(names, match.Table.Name+":"+match.Table.SubMapping)
		} else {
			names = append(names, match.Table.Name)
		}
	}
	log.Printf("[info] matches for highway=bus_stop on %s %d (closed=%t relation=%t): %s", elemType, id, closed, relation, strings.Join(names, ", "))
}

func (tm *tagMatcher) MatchNode(node *osm.Node) []Match {
	if node.ID == debugNodeID {
		tm.logDebugNodeMapping(node.Tags)
	}
	matches := tm.match(node.Tags, false, false)
	if node.ID == debugNodeID {
		tm.logBusStopMatches("node", node.ID, false, false, matches)
	}
	return matches
}

func (tm *tagMatcher) logDebugNodeMapping(tags osm.Tags) {
	highway, ok := tags["highway"]
	if !ok {
		log.Printf("[info] debug node missing highway tag; tags=%v", tags)
		return
	}
	values, ok := tm.mappings[Key("highway")]
	if !ok {
		log.Printf("[info] debug node highway=%s but no highway mapping; tags=%v", highway, tags)
		return
	}
	anyTables := destTableNames(values[Value("__any__")])
	valueTables := destTableNames(values[Value(highway)])
	log.Printf("[info] debug node highway=%s mapping: any=%v value=%v tags=%v", highway, anyTables, valueTables, tags)
}

func destTableNames(entries []orderedDestTable) []string {
	if len(entries) == 0 {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.DestTable.SubMapping != "" {
			names = append(names, entry.DestTable.Name+":"+entry.DestTable.SubMapping)
		} else {
			names = append(names, entry.DestTable.Name)
		}
	}
	return names
}

func isDebugBusStop(tags osm.Tags) bool {
	return tags["highway"] == "bus_stop" && tags["name"] == "Medzev, SOU"
}

func (tm *tagMatcher) MatchWay(way *osm.Way) []Match {
	var matches []Match
	if tm.matchAreas { // match way as polygon
		if way.IsClosed() {
			if way.Tags["area"] != "no" {
				matches = tm.match(way.Tags, true, false)
			}
		}
	} else { // match way as linestring
		if way.IsClosed() {
			if way.Tags["area"] != "yes" {
				matches = tm.match(way.Tags, true, false)
			}
		} else {
			matches = tm.match(way.Tags, false, false)
		}
	}
	return matches
}

func (tm *tagMatcher) MatchRelation(rel *osm.Relation) []Match {
	matches := tm.match(rel.Tags, true, true)
	return matches
}

type orderedMatch struct {
	Match
	order int
}

func (tm *tagMatcher) match(tags osm.Tags, closed bool, relation bool) []Match {
	type tableKeyMatches struct {
		order   int
		matches []orderedMatch
	}

	tables := make(map[DestTable]map[Key]*tableKeyMatches)

	addTableMatch := func(k, v string, t orderedDestTable) {
		keyMatches, ok := tables[t.DestTable]
		if !ok {
			keyMatches = make(map[Key]*tableKeyMatches)
			tables[t.DestTable] = keyMatches
		}

		entry, ok := keyMatches[Key(k)]
		if !ok {
			entry = &tableKeyMatches{order: t.order}
			keyMatches[Key(k)] = entry
		} else if t.order < entry.order {
			entry.order = t.order
		}

		entry.matches = append(entry.matches, orderedMatch{
			Match: Match{
				Key:     k,
				Value:   v,
				Table:   t.DestTable,
				builder: tm.tables[t.Name],
			},
			order: t.order,
		})
	}

	if values, ok := tm.mappings[Key("__any__")]; ok {
		for _, t := range values["__any__"] {
			addTableMatch("__any__", "__any__", t)
		}
	}

	for k, v := range tags {
		values, ok := tm.mappings[Key(k)]
		if ok {
			if tbls, ok := values["__any__"]; ok {
				for _, t := range tbls {
					addTableMatch(k, v, t)
				}
			}
			if tbls, ok := values[Value(v)]; ok {
				for _, t := range tbls {
					addTableMatch(k, v, t)
				}
			}
			if strings.Contains(v, ";") {
				for _, val := range splitTagValues(v) {
					if tbls, ok := values[Value(val)]; ok {
						for _, t := range tbls {
							if tm.splitValuesForTable(t.Name) {
								addTableMatch(k, val, t)
							}
						}
					}
				}
			}
		}
	}
	var matches []Match
	for t, keyMatches := range tables {
		var selected *tableKeyMatches
		for key, entry := range keyMatches {
			if !tm.multiValuesForTableKey(t.Name, key) {
				entry.matches = reduceMatches(entry.matches)
				if len(entry.matches) == 0 {
					continue
				}
				entry.order = entry.matches[0].order
			}
			if selected == nil || entry.order < selected.order {
				selected = entry
			}
		}
		if selected == nil || len(selected.matches) == 0 {
			continue
		}

		sort.SliceStable(selected.matches, func(i, j int) bool {
			return selected.matches[i].order < selected.matches[j].order
		})

		match := selected.matches[0].Match
		filters, ok := tm.filters[t.Name]
		filteredOut := false
		if ok {
			for _, filter := range filters {
				if !filter(tags, Key(match.Key), closed) {
					filteredOut = true
					if isDebugBusStop(tags) {
						log.Printf("[info] debug node filtered by table filter for %s (key=%s closed=%t relation=%t)", t.Name, match.Key, closed, relation)
					}
					break
				}
			}
		}
		if relation && !filteredOut {
			filters, ok := tm.relFilters[t.Name]
			if ok {
				for _, filter := range filters {
					if !filter(tags, Key(match.Key), closed) {
						filteredOut = true
						if isDebugBusStop(tags) {
							log.Printf("[info] debug node filtered by relation filter for %s (key=%s closed=%t)", t.Name, match.Key, closed)
						}
						break
					}
				}
			}
		}

		if !filteredOut {
			for _, selectedMatch := range selected.matches {
				matches = append(matches, selectedMatch.Match)
			}
		}
	}
	return matches
}

func (tm *tagMatcher) splitValuesForTable(tableName string) bool {
	if opt, ok := tm.valueOptions[tableName]; ok {
		return opt.splitValues
	}
	return false
}

func (tm *tagMatcher) multiValuesForTableKey(tableName string, key Key) bool {
	if opt, ok := tm.valueOptions[tableName]; ok {
		_, ok := opt.multiValues[key]
		return ok
	}
	return false
}

func reduceMatches(matches []orderedMatch) []orderedMatch {
	if len(matches) == 0 {
		return matches
	}
	best := matches[0]
	for i := 1; i < len(matches); i++ {
		if matches[i].order < best.order {
			best = matches[i]
		}
	}
	return []orderedMatch{best}
}

type valueBuilder struct {
	key     Key
	colType ColumnType
}

func (v *valueBuilder) Value(elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	if v.colType.Func != nil {
		return v.colType.Func(elem.Tags[string(v.key)], elem, geom, match)
	}
	return nil
}

func (v *valueBuilder) MemberValue(rel *osm.Relation, member *osm.Member, memberIndex int, geom *geom.Geometry, match Match) interface{} {
	if v.colType.Func != nil {
		if v.colType.FromMember {
			if member.Element == nil {
				return nil
			}
			return v.colType.Func(member.Element.Tags[string(v.key)], member.Element, geom, match)
		}
		return v.colType.Func(rel.Tags[string(v.key)], &rel.Element, geom, match)
	}
	if v.colType.MemberFunc != nil {
		return v.colType.MemberFunc(rel, member, memberIndex, match)
	}
	return nil
}

type rowBuilder struct {
	columns []valueBuilder
}

func (r *rowBuilder) MakeRow(elem *osm.Element, geom *geom.Geometry, match Match) []interface{} {
	var row []interface{}
	for _, column := range r.columns {
		row = append(row, column.Value(elem, geom, match))
	}
	return row
}

func (r *rowBuilder) MakeMemberRow(rel *osm.Relation, member *osm.Member, memberIndex int, geom *geom.Geometry, match Match) []interface{} {
	var row []interface{}
	for _, column := range r.columns {
		row = append(row, column.MemberValue(rel, member, memberIndex, geom, match))
	}
	return row
}
