"""GoogleSQL lowering of the shared native graph, with explicit semantic ports.

Parsing is an export check. Only actual BigQuery jobs establish native parity.
"""
import json
import re

import sqlglot
from sqlglot import exp
from sqlglot.transforms import eliminate_semi_and_anti_joins

from tl.stream import ValidationError
from tl.stream.events import iso
from tl.bigquery.schema import visible


def parse(sql, dialect='duckdb'):
    return sqlglot.parse_one(sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE)


def _expr(sql): return parse('SELECT '+sql, 'bigquery').expressions[0]


def _aliases(tree):
    # DuckDB permits reuse of a preceding SELECT alias; GoogleSQL does not.
    # Expand only unqualified references in the same SELECT, never child scopes.
    for select in tree.find_all(exp.Select):
        aliases = {}
        for projection in select.expressions:
            for col in list(projection.find_all(exp.Column)):
                if (not col.table and col.name in aliases
                        and col.find_ancestor(exp.Select) is select):
                    col.replace(aliases[col.name].copy())
            if isinstance(projection, exp.Alias) and not any(c.name==projection.alias for c in projection.this.find_all(exp.Column)):
                aliases[projection.alias] = projection.this.copy()
    return tree


def lower(tree):
    for window in list(tree.find_all(exp.Window)):
        fn=window.this
        if isinstance(fn,(exp.Lead,exp.Lag)) and fn.args.get('default') is not None:
            default=fn.args['default']
            if not isinstance(default,(exp.Literal,exp.Null)):
                replacement=window.copy();replacement.this.set('default',None)
                window.replace(exp.Coalesce(this=replacement,expressions=[default.copy()]))
    # GoogleSQL window ORDER BY cannot spell NULLS LAST for ascending fields.
    # An explicit null discriminator preserves DuckDB ordering for nullable keys.
    for order in tree.find_all(exp.Order):
        terms = []
        for term in order.expressions:
            if isinstance(term, exp.Ordered):
                descending = bool(term.args.get('desc'))
                nulls_first = bool(term.args.get('nulls_first'))
                if nulls_first == descending:
                    terms.append(exp.Ordered(this=exp.Is(this=term.this.copy(), expression=exp.Null()),
                                             desc=nulls_first, nulls_first=not nulls_first))
                term.set('nulls_first', not descending)
            terms.append(term)
        order.set('expressions', terms)
    def transform(node):
        if isinstance(node, exp.Select):
            # GoogleSQL has no SEMI/ANTI JOIN syntax. EXISTS preserves the
            # left-side multiplicity and SQL NULL semantics of the ON clause.
            for index, join in enumerate(node.args.get('joins') or []):
                keys = join.args.get('using')
                if join.kind not in ('SEMI', 'ANTI') or not keys:
                    continue
                # Our source checks join one named left relation. A USING key
                # after other joins needs scope qualification; never guess it.
                left = node.args['from_'].this.alias_or_name
                right = join.this.alias_or_name
                if index or not left or not right or left == right:
                    raise ValidationError('ambiguous native existence-join keys')
                join.set('on', exp.and_(*[
                    exp.column(key.name, table=left).eq(exp.column(key.name, table=right))
                    for key in keys]))
                join.set('using', None)
            return eliminate_semi_and_anti_joins(node)
        if isinstance(node, exp.Filter):
            # GoogleSQL conditional aggregates ignore NULL rather than accepting
            # DuckDB's aggregate FILTER clause. Preserve an empty SUM as NULL,
            # COUNT as zero, DISTINCT, and a predicate's UNKNOWN result.
            aggregate = node.this.copy()
            if not isinstance(aggregate, (exp.Count, exp.Sum)):
                raise ValidationError('unsupported filtered native aggregate')
            predicate = node.expression.this
            value = aggregate.this
            distinct = isinstance(value, exp.Distinct)
            if distinct:
                if len(value.expressions) != 1:
                    raise ValidationError('unsupported filtered aggregate arity')
                value = value.expressions[0]
            if isinstance(value, exp.Star):
                value = exp.Literal.number(1)
            conditional = exp.If(this=predicate.copy(), true=value.copy(), false=exp.Null())
            aggregate.set('this', exp.Distinct(expressions=[conditional]) if distinct else conditional)
            return aggregate
        if isinstance(node,exp.Interval) and isinstance(node.this,exp.Literal) and node.this.this.isdigit():
            node.set('this',exp.Literal.number(node.this.this))
        if isinstance(node,(exp.Add,exp.Sub)):
            interval=node.expression;multiplier=None
            if isinstance(interval,exp.Mul):
                if isinstance(interval.expression,exp.Interval): multiplier=interval.this;interval=interval.expression
                elif isinstance(interval.this,exp.Interval): multiplier=interval.expression;interval=interval.this
            if isinstance(interval,exp.Interval):
                amount=interval.this.sql(dialect='bigquery')
                if multiplier is not None: amount=f'({multiplier.sql(dialect="bigquery")} * {amount})'
                op='DATE_SUB' if isinstance(node,exp.Sub) else 'DATE_ADD'
                return _expr(f'{op}(CAST({node.this.sql(dialect="bigquery")} AS DATE), INTERVAL {amount} {interval.args["unit"].name})')
        if isinstance(node, exp.DataType):
            if node.this == exp.DataType.Type.INT128:
                return exp.DataType.build('BIGNUMERIC', dialect='bigquery')
            if node.this == exp.DataType.Type.DECIMAL:
                # Expression casts cannot use parameterized GoogleSQL types.
                return exp.DataType.build('NUMERIC', dialect='bigquery')
        if isinstance(node, exp.Anonymous) and node.name.lower() == 'from_json':
            source, structure = node.expressions
            fields = json.loads(structure.this)
            expressions = []
            for field, kind in fields.items():
                if not re.fullmatch(r'[a-z_]+', field): raise ValidationError('unsafe typed feature key')
                target = ('STRING' if kind == 'VARCHAR' else 'INT64' if kind == 'BIGINT'
                          else 'FLOAT64' if kind == 'DOUBLE' else 'NUMERIC' if kind.startswith('DECIMAL') else kind)
                expressions.append(f"CAST(JSON_VALUE({source.sql(dialect='bigquery')}, '$.{field}') AS {target}) AS {field}")
            return _expr('STRUCT('+','.join(expressions)+')')
        if isinstance(node, exp.SortArray):
            arg = node.this
            if isinstance(arg, exp.ArrayDistinct) and isinstance(arg.this, exp.ArrayAgg):
                value = arg.this.this.sql(dialect='bigquery')
                return _expr(f'ARRAY_AGG(DISTINCT {value} ORDER BY {value})')
            value = arg.sql(dialect='bigquery')
            return _expr(f'ARRAY(SELECT item FROM UNNEST({value}) item ORDER BY item)')
        if isinstance(node, exp.ArrayAppend):
            return _expr(f'ARRAY_CONCAT({node.this.sql(dialect="bigquery")}, [{node.expression.sql(dialect="bigquery")}])')
        if isinstance(node, exp.DateDiff):
            unit = node.args['unit'].name.upper()
            a, b = node.this.sql(dialect='bigquery'), node.expression.sql(dialect='bigquery')
            if unit in ('HOUR', 'SECOND', 'MINUTE'):
                return _expr(f'TIMESTAMP_DIFF(CAST({a} AS TIMESTAMP), CAST({b} AS TIMESTAMP), {unit})')
            return _expr(f'DATE_DIFF(CAST({a} AS DATE), CAST({b} AS DATE), {unit})')
        if isinstance(node, exp.Count) and isinstance(node.this, exp.Distinct):
            # GoogleSQL COUNT DISTINCT has one expression, not a tuple.
            args = node.this.expressions
            if len(args) == 1 and isinstance(args[0], exp.Tuple):
                vals = ','.join(arg.sql(dialect='bigquery') for arg in args[0].expressions)
                return _expr(f'COUNT(DISTINCT TO_JSON_STRING(STRUCT({vals})))')
        if isinstance(node, exp.CTE): node.set('materialized', None)
        return node
    # Bottom-up: enclosing expression SQL must see already-lowered child types.
    for node in list(tree.walk())[::-1]:
        replacement = transform(node)
        if replacement is not node: node.replace(replacement)
    return _aliases(tree)


def _flatten(tree, prefix):
    """Hoist local CTEs so recursion is at the query root, with scoped names."""
    for window in tree.find_all(exp.Window):
        if isinstance(window.this,exp.Identifier): window.set('this',exp.to_identifier(prefix+'__'+window.this.name))
        if isinstance(window.args.get('alias'),exp.Identifier): window.set('alias',exp.to_identifier(prefix+'__'+window.args['alias'].name))
    clause = tree.args.get('with_')
    if not clause: return [], tree
    names = {cte.alias: prefix+'__'+cte.alias for cte in clause.expressions}
    for table in tree.find_all(exp.Table):
        if not table.db and table.name in names:
            table.set('this', exp.to_identifier(names[table.name]))
    for col in tree.find_all(exp.Column):
        if col.table in names: col.set('table', exp.to_identifier(names[col.table]))
    lifted = []
    for cte in clause.expressions:
        columns = cte.alias_column_names
        if columns:
            base = cte.this
            while isinstance(base, exp.Union): base = base.this
            if len(columns) != len(base.expressions): raise ValidationError('recursive column contract mismatch')
            base.set('expressions', [exp.alias_(value, name) for value, name in zip(base.expressions, columns)])
        cte.set('alias', exp.TableAlias(this=exp.to_identifier(names[cte.alias])))
        cte.set('materialized', None)
        lifted.append(cte)
    tree.set('with_', None)
    return lifted, tree


def native_nodes(session):
    nodes = dict(session.graph.nodes)
    nodes['calendar'] = f'''SELECT month, DATE_ADD(month, INTERVAL 1 MONTH) AS month_end,
        DATE_DIFF(DATE_ADD(month, INTERVAL 1 MONTH),month,DAY) AS days
        FROM UNNEST(GENERATE_DATE_ARRAY(DATE '{session.first_month}',
             DATE_SUB(DATE '{session.period_end}', INTERVAL 1 DAY),INTERVAL 1 MONTH)) AS month'''
    # One edge per entity; core_errors refuses missing parents/cycles/depth overflow.
    # The bound is explicit because BigQuery recursive CTEs stop after 500 rounds.
    identity = nodes['customer_identity']
    begin, end = identity.index('walk(customer,node,path) AS ('), identity.index('), parents AS (')
    nodes['customer_identity'] = identity[:begin]+'''walk(customer,node,depth) AS (
      SELECT customer,customer,0 FROM created
      UNION ALL SELECT w.customer,e.parent,w.depth+1
      FROM walk w JOIN edges e ON e.customer=w.node
      WHERE e.parent IS NOT NULL AND w.depth<498
    '''+identity[end:]
    timeline = nodes['customer_timeline']
    start = timeline.index(', activity_times AS (')
    tail = timeline[start:]
    tail = tail.replace('FROM created c JOIN parents p USING(customer)', 'FROM @customer_identity c')
    tail = tail.replace('p.ultimate_parent', 'c.ultimate_parent')
    nodes['customer_timeline'] = 'WITH '+tail[2:]
    return nodes


def compile_query(session, sql, binding):
    nodes = native_nodes(session)
    ordered = []
    def visit(name, trail=()):
        if name in trail: raise ValidationError('cyclic native dependency')
        if name in ordered: return
        if name not in nodes: raise ValidationError('unknown native dependency: '+name)
        for dep in re.findall(r'@([a-z_]+)', nodes[name]): visit(dep, (*trail, name))
        ordered.append(name)
    for name in re.findall(r'@([a-z_]+)', sql): visit(name)
    bind = lambda text: re.sub(r'@([a-z_]+)', lambda match: 'v_'+match[1], text)
    ctes = [exp.CTE(this=parse(visible(binding, period_end=session.period_end,
                  known_at=iso(session.known_at), watermark=session.watermark), 'bigquery'),
                  alias=exp.TableAlias(this=exp.to_identifier('_visible')))]
    for name in ordered:
        dialect = 'bigquery' if name == 'calendar' else 'duckdb'
        tree = parse(bind(nodes[name]), dialect)
        if name=='journals':
            expanded=next(cte for cte in tree.args['with_'].expressions if cte.alias=='expanded')
            # Parse this one paired expansion in the target grammar; parsing its
            # OFFSET alias as DuckDB would silently drop the scalar alias.
            expanded.set('this',parse('''SELECT activity_id,_source,customer,month,account,
                amounts[OFFSET(posting_index)] AS amount_cents,upstream_ids
                FROM postings CROSS JOIN UNNEST(accounts) AS account WITH OFFSET AS posting_index''','bigquery'))
        nested, tree = _flatten(tree, 'v_'+name)
        ctes.extend(nested)
        ctes.append(exp.CTE(this=tree, alias=exp.TableAlias(this=exp.to_identifier('v_'+name))))
    tree = parse(bind(sql))
    nested, tree = _flatten(tree, 'requested')
    ctes.extend(nested)
    tree.set('with_', exp.With(expressions=ctes, recursive=True))
    result = lower(tree).sql(dialect='bigquery', pretty=True, unsupported_level=sqlglot.ErrorLevel.RAISE)
    # Reject known untranslated constructs rather than emitting plausible SQL.
    forbidden = r'\b(FROM_JSON|SORT_ARRAY|ARRAY_DISTINCT|ARRAY_APPEND|INT128|HUGEINT|GENERATE_ARRAY)\s*(\(|\b)'
    if re.search(forbidden, result, re.I): raise ValidationError('unlowered native SQL construct')
    if any(join.kind in ('SEMI', 'ANTI') for join in tree.find_all(exp.Join)):
        raise ValidationError('unlowered native semi/anti join')
    if tree.find(exp.Filter):
        raise ValidationError('unlowered native aggregate filter')
    parse(result, 'bigquery')
    return result, ordered
