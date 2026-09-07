"""Explicit dependency expansion. No stored intermediate reporting objects."""
import json
import re
from pathlib import Path

from tl.stream import ValidationError


class Graph:
    def __init__(self):
        self.nodes = {}

    def add(self, name, sql):
        if name in self.nodes:
            raise ValidationError('duplicate query dependency: '+name)
        self.nodes[name] = sql.strip().rstrip(';')

    def typed(self, name, activities, fields):
        structure = json.dumps(fields).replace("'", "''")
        values = ','.join("'"+kind+"'" for kind in activities)
        self.add(name, f'''SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{structure}') AS f
            FROM _visible WHERE activity IN ({values})''')

    def compile(self, sql):
        ordered = []
        def visit(name, trail=()):
            if name in trail:
                raise ValidationError('cyclic query dependency: '+name)
            if name not in self.nodes:
                raise ValidationError('missing query dependency: '+name)
            if name in ordered:
                return
            for dep in re.findall(r'@([a-z_]+)', self.nodes[name]):
                visit(dep, (*trail, name))
            ordered.append(name)
        for name in re.findall(r'@([a-z_]+)', sql):
            visit(name)
        def bind(value):
            return re.sub(r'@([a-z_]+)', lambda m: 'v_'+m[1], value)
        # DuckDB 1.2 otherwise expands repeated references as independent scans.
        # Reuse is bounded to this query; nothing survives as a stored table.
        references = [ref for value in [sql, *(self.nodes[n] for n in ordered)]
                      for ref in re.findall(r'@([a-z_]+)', value)]
        ctes = ',\n'.join('v_'+name+' AS '+('MATERIALIZED ' if references.count(name)>1 else '')+
                          '('+bind(self.nodes[name])+')' for name in ordered)
        return ('WITH RECURSIVE '+ctes+'\nSELECT * FROM ('+bind(sql)+') AS requested_output' if ctes else sql), ordered

    def file(self, name, path):
        self.add(name, Path(path).read_text(encoding='utf-8'))
