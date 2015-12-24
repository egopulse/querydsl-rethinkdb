package com.egopulse.querydsl.rethinkdb;

import com.querydsl.core.types.*;
import com.rethinkdb.gen.ast.ReqlExpr;

// TODO: why querydsl-mongodb implementation sometimes use `visit`, sometimes use `handle`, something expression#accept
// when it needs to do recursion? For now I just blindly follow it
public class RethinkDBSerializer implements Visitor<Object, ReqlExpr> {

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
            return expr.getConstant();
        }
    }

    private ReqlExpr circle(Expression<?> expr, ReqlExpr context) {
        return (ReqlExpr) expr.accept(this, context);
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


}
