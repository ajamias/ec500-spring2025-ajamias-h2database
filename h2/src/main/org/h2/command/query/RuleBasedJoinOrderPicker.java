package org.h2.command.query;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.table.TableFilter;

/**
 * Determines the best join order by following rules rather than considering every possible permutation.
 */
public class RuleBasedJoinOrderPicker {
    final SessionLocal session;
    final TableFilter[] filters;

    public RuleBasedJoinOrderPicker(SessionLocal session, TableFilter[] filters) {
        this.session = session;
        this.filters = filters;
    }

    public TableFilter[] bestOrder() {
        Arrays.sort(filters, Comparator.comparingLong(o -> o.getTable().getRowCountApproximation(session)));

        String sql_expression = filters[0].getFullCondition().getSQL(0, Expression.AUTO_PARENTHESES);
        String[] join_expression_lines = Arrays.stream(sql_expression.split("\n"))
                .filter(str -> (str.chars().filter(c -> c == '\"').count() == 8))
                .toArray(String[]::new);

        for (int i = 0; i < join_expression_lines.length; ++i) {
            System.out.println(join_expression_lines[i]);
        }

        Map<String, Set<String>> alias_map = new HashMap<>();
        for (int i = 0; i < join_expression_lines.length; ++i) {
            String line = join_expression_lines[i];
            int alias_1_start_idx = line.indexOf("\"");
            int alias_1_end_idx = line.indexOf("\"", alias_1_start_idx + 1);
            int alias_2_start_idx = line.indexOf("\"", line.indexOf("="));
            int alias_2_end_idx = line.indexOf("\"", alias_2_start_idx + 1);

            String alias_1 = line.substring(alias_1_start_idx + 1, alias_1_end_idx);
            String alias_2 = line.substring(alias_2_start_idx + 1, alias_2_end_idx);

            alias_map.computeIfAbsent(alias_1, k -> new HashSet<>()).add(alias_2);
            alias_map.computeIfAbsent(alias_2, k -> new HashSet<>()).add(alias_1);
        }

        for (int i = 0; i < filters.length - 2; ++i) {
            for (int j = i + 1; j < filters.length; ++j) {
                String alias_1 = filters[i].getTable().getName();
                alias_1 = alias_1.substring(alias_1.indexOf(".") + 1);

                String alias_2 = filters[j].getTable().getName();
                alias_2 = alias_2.substring(alias_2.indexOf(".") + 1);

                if (alias_map.get(alias_1).contains(alias_2)) {
                    TableFilter temp = filters[j];
                    for (int k = j; k > i + 1; --k) {
                        filters[k] = filters[k - 1];
                    }
                    filters[i + 1] = temp;
                    break;
                }
            }
        }

        return filters;
    }
}