using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace QueryHelper
{
    public class BaseSqlStatement
    {
        public BaseSqlStatement(StringBuilder stringBuilder)
        {
            StringQuery = stringBuilder;
        }
        public BaseSqlStatement()
        { StringQuery = new StringBuilder(); }

        public BaseSqlStatement(string query, object model)
        {
            Model = model;
            StringQuery = new StringBuilder(query);
        }

        protected StringBuilder StringQuery { get; set; }
        public BaseSqlStatement SetModel(object model)
        {
            Model = model;
            return this;
        }
        public string Query
        {
            get => StringQuery.ToString();
        }

        public BaseSqlStatement(StringBuilder strBuilder, object model)
        {
            StringQuery = strBuilder;
            Model = model;
        }
        public object Model { get; private set; }


        public static BaseSqlStatement SetQueryAndModel(string query, object model)
        {
            return new BaseSqlStatement(query, model);
        }

    }

    public class InsertStatement : BaseSqlStatement
    {
        public InsertStatement(string query, object model) : base(query, model)
        { }

        public InsertStatement(string query)
        {
            StringQuery = new StringBuilder(query);
        }
    }

    public class DeleteStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public DeleteStatement(StringBuilder stringBuilder) : base(stringBuilder)
        { }

        public DeleteStatement(StringBuilder stringBuilder, object model) : base(stringBuilder, model)
        { }
        public WhereStatement<TEntity> Where(string propName)
        {
            StringQuery.Append($" WHERE {propName} = @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereFor(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} = @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLikeFor(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} LIKE @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }
    }

    public class SelectStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public SelectStatement(StringBuilder stringBuilder) : base(stringBuilder)
        { }

        public SelectStatement(StringBuilder stringBuilder, object model) : base(stringBuilder, model)
        {
        }

        public WhereStatement<TEntity> Where(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {prop} = @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {prop} LIKE @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> Where(string prop)
        {
            StringQuery.Append($" WHERE {prop} = @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(string prop)
        {
            StringQuery.Append($" WHERE {prop} LIKE @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public GroupStatement<TEntity> GroupBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<TEntity>(StringQuery, Model);
        }
    }

    public class SelectDistinctStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public SelectDistinctStatement(StringBuilder sb) : base(sb)
        { }

        public SelectDistinctStatement(StringBuilder sb, object model) : base(sb, model)
        { }

        public WhereStatement<TEntity> Where(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {prop} = @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {prop} LIKE @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> Where(string prop)
        {
            StringQuery.Append($" WHERE {prop} = @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(string prop)
        {
            StringQuery.Append($" WHERE {prop} LIKE @{prop}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Groups the by.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">You have to provide {prop} property in the select</exception>
        public GroupStatement<TEntity> GroupBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            if (!Query.Contains(prop))
                throw new ArgumentException($"You have to provide {prop} property in the SELECT");
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Groups the by.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public GroupStatement<TEntity> GroupBy(params Expression<Func<TEntity, object>>[] param)
        {
            var props = param.Aggregate(string.Empty, (str, par) =>
            {
                var prop = par.GetExpressionParameter();
                if (!Query.Contains(prop))
                    throw new ArgumentException($"You have to provide {prop} property in the SELECT");
                return string.IsNullOrEmpty(str) ? prop : $"{str}, {prop}";
            });
            StringQuery.Append($" GROUP BY {props}");
            return new GroupStatement<TEntity>(StringQuery, Model);
        }
    }

    public class UpdateStatement<TEntity> : BaseSqlStatement where TEntity : class
    {

        public UpdateStatement(string query, object model) : base(query, model)
        { }

        public UpdateStatement(StringBuilder sb) : base(sb)
        {

        }

        public UpdateStatement(StringBuilder sb, object model) : base(sb, model)
        {

        }
        public WhereStatement<TEntity> WhereEquals(string propName)
        {
            StringQuery.Append($"WHERE {propName} = @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereEqualsFor(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"WHERE {propName} = @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(string propName)
        {
            StringQuery.Append($"WHERE {propName} LIKE %@{propName}%");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLikeFor(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"WHERE {propName} LIKE %@{propName}%");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }
    }

    public class WhereStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public WhereStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }
        public WhereStatement(string query, object model) : base(query, model)
        { }

        public AndStatement<TEntity> And(string propName)
        {
            StringQuery.Append($" AND {propName} = @{propName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> AndLike(string propName)
        {
            StringQuery.Append($" AND {propName} LIKE %@{propName}%");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> And(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{propName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> AndLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE %@{propName}%");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($"OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE %@{propName}%");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE %@{propName}%");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrderByStatement<TEntity> OrderBy(string propName)
        {
            StringQuery.Append($" ORDER BY {propName}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByStatement<TEntity> OrderBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" ORDER BY {prop}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(string propName)
        {
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }

        public GroupStatement<TEntity> GroupBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<TEntity>(StringQuery, Model);
        }
    }

    public class AndStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public AndStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }

        public AndStatement(string query, object model) : base(query, model)
        { }

        public AndStatement<TEntity> And(string propName)
        {
            StringQuery.Append($" AND {propName} = @{propName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> AndLike(string propName)
        {
            StringQuery.Append($" AND {propName} LIKE %@{propName}%");
            return new AndStatement<TEntity>(StringQuery, Model);
        }


        public AndStatement<TEntity> And(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{propName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> AndLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE %@{propName}%");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE %@{propName}%");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE %@{propName}%");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrderByStatement<TEntity> OrderBy(string propName)
        {
            StringQuery.Append($" ORDER BY {propName}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(string propName)
        {
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }

        public GroupStatement<TEntity> GroupBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<TEntity>(StringQuery, Model);
        }
    }

    public class OrStatement<TEntity> : BaseSqlStatement where TEntity : class
    {

        public OrStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }
        public OrStatement(string query, object model) : base(query, model)
        { }

        public AndStatement<TEntity> And(string propName)
        {
            StringQuery.Append($" AND {propName} = @{propName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> AndLike(string propName)
        {
            StringQuery.Append($" AND {propName} LIKE %@{propName}%");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> And(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{propName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public AndStatement<TEntity> AndLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE %@{propName}%");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE %@{propName}%");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE %@{propName}%");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrderByStatement<TEntity> OrderBy(string propName)
        {
            StringQuery.Append($" ORDER BY {propName}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(string propName)
        {
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }

        public GroupStatement<TEntity> GroupBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<TEntity>(StringQuery, Model);
        }
    }

    public class OrderByStatement<TEntity> : BaseSqlStatement where TEntity : class
    {

        public OrderByStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }
        public OrderByStatement(string query, object model) : base(query, model)
        { }
        public OrderByStatement<TEntity> OrderBy(string propName)
        {
            StringQuery.Append($", {propName}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(string propName)
        {
            StringQuery.Append($", {propName} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }
    }

    public class OrderByDescendingStatement<TEntity> : BaseSqlStatement where TEntity : class
    {

        public OrderByDescendingStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }
        public OrderByDescendingStatement(string query, object model) : base(query, model)
        { }
        public OrderByStatement<TEntity> OrderBy(string propName)
        {
            StringQuery.Append($", {propName}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(string propName)
        {
            StringQuery.Append($", {propName} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }

        public PageStatement Page()
        {
            StringQuery.Append("OFFSET (@Offset) ROWS FETCH NEXT (@limit) ROWS ONLY");
            return new PageStatement(StringQuery, Model);
        }
    }

    public class GroupStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public GroupStatement(StringBuilder sb, object model) : base(sb, model)
        { }

        public GroupStatement(StringBuilder sb) : base(sb)
        { }

        public OrderByStatement<TEntity> OrderBy(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" ORDER BY {prop}");
            return new OrderByStatement<TEntity>(StringQuery, Model);
        }

        public OrderByDescendingStatement<TEntity> OrderByDescending(Expression<Func<TEntity, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" ORDER BY {prop} DESC");
            return new OrderByDescendingStatement<TEntity>(StringQuery, Model);
        }
    }

    public class PageStatement : BaseSqlStatement
    {
        public PageStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }

        public PageStatement(string query, object model) : base(query, model)
        { }
    }
}