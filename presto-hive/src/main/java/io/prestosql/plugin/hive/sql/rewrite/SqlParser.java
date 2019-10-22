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

import io.airlift.log.Logger;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

/**
 *
 * SqlParser receive Hive sql and it can transform to other sql dialect such as Presto sql.
 * Also a SqlParser can check whether a text sql is validated.
 *
 * Created by wujianchao on 2019/9/10.
 */
public class SqlParser {

    private static final Logger log = Logger.get(SqlParser.class);

    public static String rewrite(String sql, List<Function<TokenStream, Rewrite>> rewrites){
        long startTime = System.currentTimeMillis();
        String rewriteSql = sql;

        for(Function<TokenStream, Rewrite> rewrite : rewrites){
            rewriteSql = rewrite(rewriteSql, rewrite);
        }

        long endTime = System.currentTimeMillis();
        log.debug("sql rewrite time cost {} ms", (endTime - startTime));

        return rewriteSql;
    }

    private static String rewrite(String sql, Function<TokenStream, Rewrite> rewriteSupplier) {
        String rewriteName = "";
        try {
            // generate token stream
            OsqlBaseLexer lexer = new OsqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
            TokenStream tokenStream = new CommonTokenStream(lexer);

            Rewrite rewrite = rewriteSupplier.apply(tokenStream);
            rewriteName = rewrite.getClass().getSimpleName();

            OsqlBaseParser parser = new OsqlBaseParser(tokenStream);

            // generate AST
            ParseTree ast = parser.singleStatement();

            // traverse AST
            new ParseTreeWalker().walk(rewrite, ast);
            return rewrite.rewritedSql();

        }catch (Exception e){
            log.warn(String.format("%s failed to rewrite sql", rewriteName), e);
            return sql;
        }
    }

    public abstract static class Rewrite extends OsqlBaseBaseListener {

        protected TokenStreamRewriter tokenStreamRewriter;

        public Rewrite(TokenStream tokenStream){
            //generate token stream
            this.tokenStreamRewriter = new TokenStreamRewriter(tokenStream);
        }

        protected String rewritedSql(){
            return tokenStreamRewriter.getText();
        }

        //-------------utils methods---------------

        /**
         * text of token
         */
        protected String tokenText(Token token){
            return token.getText();
        }

        /**
         * text of node
         */
        protected String nodeText(ParseTree node){
            return node.getText();
        }

        /**
         * delete token and subsequent blank
         */
        protected void deleteToken(Token token) {
            tokenStreamRewriter.delete(token);
            int nextToken = token.getTokenIndex() + 1;
            if(blankToken(nextToken)){
                tokenStreamRewriter.delete(nextToken);
            }
        }

        /**
         * Find the first token from start of a token stream.
         */
        protected int tokenIndex(int start, String token){
            int totalToken = tokenStreamRewriter.getTokenStream().size();
            for(int i = start; i < totalToken; i++){
                String text = tokenStreamRewriter.getTokenStream().get(i).getText();
                if(Objects.equals(token.toLowerCase(Locale.getDefault()), text.toLowerCase(Locale.getDefault()))){
                    return i;
                }
            }
            return -1;
        }

        /**
         * whether a token of index i is start with blank
         */
        protected boolean blankToken(int i) {
            return tokenStreamRewriter.getTokenStream().get(i).getText().startsWith(Constants.BLANK);
        }

    }


}
