/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.sql.rewrite;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.TerminalNode;

import static io.prestosql.plugin.hive.sql.rewrite.Constants.BLANK;
import static io.prestosql.plugin.hive.sql.rewrite.Constants.COMMA;
import static io.prestosql.plugin.hive.sql.rewrite.Constants.LEFT_PARENTHESIS;
import static io.prestosql.plugin.hive.sql.rewrite.Constants.RIGHT_PARENTHESIS;

/**
 * Created by wujianchao on 2019/9/10.
 */
public class PrestoRewrite extends SqlParser.Rewrite {

    public PrestoRewrite(TokenStream tokenStream) {
        super(tokenStream);
    }

    @Override
    public void enterPredicated(OsqlBaseParser.PredicatedContext ctx) {
        super.enterPredicated(ctx);
    }

    /**
     * Hive support regexp and rlike operators, but Presto does not
     *
     * Resolution:
     *  col regexp '.*' =>  regexp_like(col, '.*')
     *  col rlike '.*' =>  regexp_like(col, '.*')
     *  col not regexp '.*' =>  not regexp_like(col, '.*')
     *  col not rlike '.*' =>  not regexp_like(col, '.*')
     *  not col not rlike '.*' =>  not not regexp_like(col, '.*')
     */
    @Override
    public void enterPredicate(OsqlBaseParser.PredicateContext ctx) {
        String kind = ctx.kind.getText().toUpperCase();
        if("REGEXP".equals(kind) || "RLIKE".equals(kind)){
            ParserRuleContext parent = ctx.getParent();

            tokenStreamRewriter.insertBefore(parent.start, LEFT_PARENTHESIS);
            tokenStreamRewriter.insertBefore(parent.start, "regexp_like");
            tokenStreamRewriter.insertAfter(((ParserRuleContext)(parent.getChild(0))).stop, COMMA);

            if(ctx.getChildCount() == 3){
                // case : col not regexp '.*'
                tokenStreamRewriter.insertBefore(parent.start, BLANK);
                tokenStreamRewriter.insertBefore(parent.start, "not");
                deleteTokenWithNextBlank(((TerminalNode) (ctx.getChild(0))).getSymbol());
                deleteTokenWithNextBlank(((TerminalNode) (ctx.getChild(1))).getSymbol());
            }else {
                // case : col regexp '.*'
                deleteTokenWithNextBlank(((TerminalNode) (ctx.getChild(0))).getSymbol());
            }
            tokenStreamRewriter.insertAfter(ctx.stop, RIGHT_PARENTHESIS);
        }
    }

    /**
     * Hive support % operator, but Presto does not.
     *
     * Resolution:
     *  1 % 2 => MOD(1, 2)
     */
    @Override
    public void enterArithmeticBinary(OsqlBaseParser.ArithmeticBinaryContext ctx) {
        String operator = ctx.operator.getText();
        if("%".equals(operator)){
            tokenStreamRewriter.insertBefore(ctx.start, "mod(");
            deleteTokenWithNextBlank(ctx.operator);
            tokenStreamRewriter.insertAfter(ctx.left.stop, COMMA);
            tokenStreamRewriter.insertAfter(ctx.stop, RIGHT_PARENTHESIS);
        }
    }

    /**
     * Resolution:
     *  1. array(1, 2) => array[1, 2]
     *  2. concat('a','b','c') => 'a' || 'b' || 'c'
     */
    @Override
    public void enterFunctionCall(OsqlBaseParser.FunctionCallContext ctx) {

        if ("ARRAY".equals(ctx.getChild(0).getText().toUpperCase())){
            tokenStreamRewriter.replace(((TerminalNode)(ctx.getChild(1))).getSymbol(), "[");
            tokenStreamRewriter.replace(ctx.stop, "]");
        }
    }

    /**
     * Hive string type is string, Presto is varchar
     *
     * Resolution:
     *  string => varchar
     */
    @Override
    public void enterPrimitiveDataType(OsqlBaseParser.PrimitiveDataTypeContext ctx) {
        if("STRING".equals(ctx.start.getText().toUpperCase())){
            tokenStreamRewriter.replace(ctx.start, "varchar");
        }
    }

    /**
     * Hive and Presto have different LATERAL VIEW syntax
     *   Hive: LATERAL VIEW explode(scores) t score
     *   Presto: cross join unnest(scores) as t (score)
     */
    @Override
    public void enterLateralView(OsqlBaseParser.LateralViewContext ctx) {
        if(!"EXPLODE".equals(nodeText(ctx.funcName).toUpperCase())){
            //TODO sql transform exception
            throw new RuntimeException("Lateral View query olny support UDTF explode");
        }

        tokenStreamRewriter.replace(ctx.start, ctx.funcName.stop, "cross join unnest");
        tokenStreamRewriter.insertBefore(ctx.tblName.start, "as ");

        if(ctx.AS() != null) {
            deleteTokenWithNextBlank(ctx.AS().getSymbol());
        }

        if(ctx.colName != null && !ctx.colName.isEmpty()){
            tokenStreamRewriter.insertBefore(ctx.colName.get(0).start, LEFT_PARENTHESIS);
            tokenStreamRewriter.insertAfter(ctx.colName.get(ctx.colName.size() -1).stop, RIGHT_PARENTHESIS);
        }

    }

    /**
     * Hive identifier quoter is ` but Presto identifier quoter is "
     *
     * Resolution:
     *  `col` => "col"
     */
    @Override
    public void enterQuotedIdentifier(OsqlBaseParser.QuotedIdentifierContext ctx) {
        String identifier = ctx.getText();
        char[] chars = new char[identifier.length()];
        identifier.getChars(0, identifier.length(), chars, 0);
        chars[0] = '"';
        chars[chars.length -1] = '"';
        tokenStreamRewriter.replace(ctx.start, new String(chars));
    }

    /**
     * Hive identifier can start with digit, but Presto can not
     *
     * Resolution:
     *  1var => "1var"
     */
    @Override
    public void enterUnquotedIdentifier(OsqlBaseParser.UnquotedIdentifierContext ctx) {
        String identifier = ctx.getText();
        char firstChar = identifier.charAt(0);
        if(firstChar >= '0' && firstChar <= '9'){
            tokenStreamRewriter.replace(ctx.start, "\"" + identifier + "\"");
        }
    }

    /**
     * Hive supports syntax cluster-by, distribute-by and sort-by etc. but Presto does not
     *
     * Resolution:
     *  1. delete cluster-by and distribute-by clause.
     *  2. replace sort by with order by
     */
    @Override
    public void exitQueryOrganization(OsqlBaseParser.QueryOrganizationContext ctx) {
        if(ctx.clusterBy != null && !ctx.clusterBy.isEmpty()){
            tokenStreamRewriter.delete(ctx.CLUSTER().getSymbol(), ctx.expression.stop);
        }
        if(ctx.distributeBy != null && !ctx.distributeBy.isEmpty()){
            tokenStreamRewriter.delete(ctx.DISTRIBUTE().getSymbol(), ctx.expression.stop);
        }
        if(ctx.SORT() != null){
            tokenStreamRewriter.replace(ctx.SORT().getSymbol(), "order");
        }
    }

    /**
     * If function name contain ".", presto must quote it.
     */
    @Override
    public void exitFunctionCall(OsqlBaseParser.FunctionCallContext ctx) {
        OsqlBaseParser.QualifiedNameContext functionName = (OsqlBaseParser.QualifiedNameContext) ctx.getChild(0);
        if(functionName.getChildCount() > 1){
            tokenStreamRewriter.replace(functionName.start, functionName.stop, "\"" + nodeText(functionName) + "\"");
        }
    }

    /**
     * Hive quotes string literal with both " and ', but Presto only "
     *
     * Resolution:
     *  replace " with '
     */
    @Override
    public void exitStringLiteral(OsqlBaseParser.StringLiteralContext ctx) {
        for (TerminalNode stringLiteral : ctx.STRING()){
            String value = stringLiteral.getText();
            if(value.startsWith("\"")){
                tokenStreamRewriter.replace(stringLiteral.getSymbol(),
                        "'" + value.substring(1, value.length() - 1) + "'");
            }
        }
    }

}
