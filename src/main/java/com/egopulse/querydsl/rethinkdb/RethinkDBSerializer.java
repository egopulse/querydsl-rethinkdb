package com.egopulse.querydsl.rethinkdb;

import com.querydsl.core.types.*;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.OrderBy;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.model.Arguments;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Naive rough implementation of the serializer from QueryDSL expression to ReQL expression
 *
 * TODO:
 *  - document what is different from ordinary QueryDSL
 *  - why querydsl-mongodb implementation sometimes use `visit`, sometimes use `handle`, something expression#accept
 *    when it needs to do recursion? For now I just blindly follow it
 *  - lazily fetching cursor
 */
public class RethinkDBSerializer implements Visitor<Object, ReqlExpr> {

    private static final RethinkDB r = RethinkDB.r;

    public Object handle(Expression<?> expression, ReqlExpr context) {
        return circle(expression, context);
    }

    @Override
    public Object visit(Constant<?> expr, ReqlExpr context) {
        if (Enum.class.isAssignableFrom(expr.getType())) {
            @SuppressWarnings("unchecked") //Guarded by previous check
                    Constant<? extends Enum<?>> expectedExpr = (Constant<? extends Enum<?>>) expr;
            return expectedExpr.getConstant().name();
        } else {
            return r.expr(expr.getConstant());
        }
    }

    private ReqlExpr circle(Expression<?> expr, ReqlExpr context) {
        return (ReqlExpr) expr.accept(this, context);
    }

    public ReqlExpr toSort(List<OrderSpecifier<?>> orderSpecs, ReqlExpr context) {
        Arguments args = new Arguments(context);

        for (OrderSpecifier orderSpec : orderSpecs) {
            args.coerceAndAdd(
                    orderSpec.isAscending()
                            ? r.asc(((Path) orderSpec.getTarget()).getMetadata().getName())
                            : r.desc(((Path) orderSpec.getTarget()).getMetadata().getName()));
        }

        return new OrderBy(args);
    }

    @Override
    public Object visit(FactoryExpression<?> expr, ReqlExpr context) {
        return null;
    }

    @Override
    public Object visit(Operation<?> expr, ReqlExpr context) {
        Operator op = expr.getOperator();

        if (op == Ops.EQ) {
            if (expr.getArg(0) instanceof Operation) {
                Operation<?> lhs = (Operation<?>) expr.getArg(0);
                if (lhs.getOperator() == Ops.COL_SIZE || lhs.getOperator() == Ops.ARRAY_SIZE) {
                    // TODO: is there a counter part of this on RethinkDB? (it is `$size` with MongoDB)
                    throw new UnsupportedOperationException("You hit a todo item, check out next time");
                } else {
                    throw new UnsupportedOperationException("Illegal operation " + expr);
                }
            } else if (expr.getArg(0) instanceof Path) {
                Path<?> path = (Path<?>) expr.getArg(0);
                Object constantValue = ((Constant<?>) expr.getArg(1)).getConstant();
                return circle(path, context).eq(constantValue);
            }

        } else if (op == Ops.STRING_IS_EMPTY) {
            return circle(expr.getArg(0), context).eq("");

        } else if (op == Ops.AND) {
            return circle(expr.getArg(0), context)
                    .and(circle(expr.getArg(1), context));

        } else if (op == Ops.NOT) {
            //Handle the not's child
            Operation<?> subOperation = (Operation<?>) expr.getArg(0);
            Operator subOp = subOperation.getOperator();
            if (subOp == Ops.IN) {
                return visit(ExpressionUtils.operation(Boolean.class, Ops.NOT_IN, subOperation.getArg(0),
                        subOperation.getArg(1)), context);
            } else {
                return circle(expr.getArg(0), context).not();
            }

        } else if (op == Ops.OR) {
            return circle(expr.getArg(0), context)
                    .or(circle(expr.getArg(1), context));

        } else if (op == Ops.NE) {
            Path<?> path = (Path<?>) expr.getArg(0);
            Constant<?> constant = (Constant<?>) expr.getArg(1);
            return circle(path, context).ne(constant);

        } else if (op == Ops.STARTS_WITH) {
            return circle(expr.getArg(0), context).match("^" + regexQuote(expr.getArg(1)));

        } else if (op == Ops.STARTS_WITH_IC) {
            return circle(expr.getArg(0), context).match("(?i)^" + regexQuote(expr.getArg(1)));

        } else if (op == Ops.ENDS_WITH) {
            return circle(expr.getArg(0), context).match(regexQuote(expr.getArg(1)) + "$");

        } else if (op == Ops.ENDS_WITH_IC) {
            return circle(expr.getArg(0), context).match("(?i)" + regexQuote(expr.getArg(1)) + "$");

        } else if (op == Ops.EQ_IGNORE_CASE) {
            return circle(expr.getArg(0), context).match("(?i)" + "^" + regexQuote(expr.getArg(1)) + "$");

        } else if (op == Ops.STRING_CONTAINS) {
            return circle(expr.getArg(0), context).match(".*" + regexQuote(expr.getArg(1)) + ".*");

        } else if (op == Ops.STRING_CONTAINS_IC) {
            return circle(expr.getArg(0), context).match("(?).*" + regexQuote(expr.getArg(1)) + ".*");

        } else if (op == Ops.MATCHES) {
            return circle(expr.getArg(0), context).match(expr.getArg(1).toString());

        } else if (op == Ops.MATCHES_IC) {
            return circle(expr.getArg(0), context).match("(?)" + expr.getArg(1).toString());

        } else if (op == Ops.LIKE) {
            String regex = ExpressionUtils.likeToRegex((Expression) expr.getArg(1)).toString();
            return circle(expr.getArg(0), context).match(regex);

        } else if (op == Ops.BETWEEN) {
            ReqlExpr middle = circle(expr.getArg(0), context);
            ReqlExpr lowerBound = circle(expr.getArg(1), context);
            ReqlExpr upperBound = circle(expr.getArg(2), context);
            return middle
                    .gt(lowerBound)
                    .and(middle.lt(upperBound));

        } else if (op == Ops.IN) {
            return circle(expr.getArg(1), context).contains(circle(expr.getArg(0), context));

        } else if (op == Ops.NOT_IN) {
            return circle(expr.getArg(1), context).contains(circle(expr.getArg(0), context)).not();

        } else if (op == Ops.COL_IS_EMPTY) {
            return circle(expr.getArg(0), context).isEmpty();

        } else if (op == Ops.LT) {
            return circle(expr.getArg(0), context).lt(circle(expr.getArg(1), context));

        } else if (op == Ops.GT) {
            return circle(expr.getArg(0), context).gt(circle(expr.getArg(1), context));

        } else if (op == Ops.LOE) {
            return circle(expr.getArg(0), context)
                    .lt(circle(expr.getArg(1), context))
                    .or(circle(expr.getArg(0), context).eq(circle(expr.getArg(1), context)));

        } else if (op == Ops.GOE) {
            return circle(expr.getArg(0), context)
                    .gt(circle(expr.getArg(1), context))
                    .or(circle(expr.getArg(0), context).eq(circle(expr.getArg(1), context)));

            // ??? The chunk below doesn't work
            // Supplier<ReqlExpr> leftOperand = () -> circle(expr.getArg(0), context);
            // Supplier<ReqlExpr> rightOperand = () -> circle(expr.getArg(1), context);
            // return leftOperand.get()
            //         .gt(rightOperand.get(), context)
            //         .or(leftOperand.get(), context).eq(rightOperand.get(), context);

        } else if (op == Ops.IS_NULL) {
            return context.hasFields(leafName((Path<?>) expr.getArg(0))).not();

        } else if (op == Ops.IS_NOT_NULL) {

            return context.hasFields(leafName((Path<?>) expr.getArg(0)));
            // !!! Can't found the corresponding methods on the generated classes

        } else if (op == Ops.CONTAINS_KEY) {
            Path<?> path = (Path<?>) expr.getArg(0);
            Expression<?> key = expr.getArg(1);
            return circle(path, context).hasFields(circle(key, context));

        } else {
            throw new UnsupportedOperationException();
        }
        return null;
    }

    @Override
    public ReqlExpr visit(Path<?> expr, ReqlExpr context) {
        PathMetadata metadata = expr.getMetadata();
        if (metadata.getParent() != null) {
            Path<?> parent = metadata.getParent();
            if (parent.getMetadata().getPathType() == PathType.DELEGATE) {
                parent = parent.getMetadata().getParent();
            }
            if (metadata.getPathType() == PathType.COLLECTION_ANY) {
                return visit(parent, context);
            } else if (parent.getMetadata().getPathType() != PathType.VARIABLE) {
                return visit(parent, context).getField(context.getField(metadata.getName()));
            }
        }
        return context.getField(metadata.getName());
    }

    @Override
    public Object visit(SubQueryExpression<?> expr, ReqlExpr context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(TemplateExpression<?> expr, ReqlExpr context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(ParamExpression<?> expr, ReqlExpr context) {
        throw new UnsupportedOperationException();
    }

    private String regexQuote(Expression<?> expr) {
        return Pattern.quote(expr.toString());
    }

    private String leafName(Path<?> path) {
        return path.getMetadata().getName();
    }

}
