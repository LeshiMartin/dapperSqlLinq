using System;
using System.Collections.Generic;
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
            _model = model;
            if (model != null)
                ModelValues = model.AsDictionary();
            StringQuery = new StringBuilder(query);
        }

        protected StringBuilder StringQuery { get; set; }

        public BaseSqlStatement SetModel(object model)
        {
            _model = model;
            ModelValues = model.AsDictionary();
            return this;
        }

        public string Query
        {
            get => StringQuery.ToString();
        }

        public BaseSqlStatement(StringBuilder strBuilder, object model)
        {
            StringQuery = strBuilder;
            _model = model;
            if (model != null)
                ModelValues = model.AsDictionary();
        }

        private object _model;

        public object Model => ModelValues?.ToObject(_model);

        public static BaseSqlStatement SetQueryAndModel(string query, object model)
        {
            return new BaseSqlStatement(query, model);
        }

        protected IDictionary<string, object> ModelValues { get; set; }

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
            StringQuery.Append($" WHERE {prop} LIKE @{prop} ");
            ModelValues[prop] = "%" + ModelValues[prop] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the not in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereNotIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} NOT IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> Where<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} = @{paramName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
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

    public class JoinStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public JoinStatement(StringBuilder sb) : base(sb)
        { }

        public JoinStatement(StringBuilder sb, object model) : base(sb, model)
        {

        }
        public JoinStatement<T2> Join<T2>(Expression<Func<TEntity, object>> innerKey, Expression<Func<T2, object>> outerKey) where T2 : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            StringQuery.AppendLine(
                $"JOIN dbo.{typeof(T2).Name} AS {typeof(T2).Name} ON {typeof(TEntity).Name}.{innKey} = {typeof(T2).Name}.{outKey}");

            return new JoinStatement<T2>(StringQuery, Model);
        }

        public JoinStatement<T2> Join<T2>(Expression<Func<TEntity, object>> innerKey, Expression<Func<T2, object>> outerKey, string outerTable) where T2 : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            StringQuery.AppendLine(
                $"JOIN {outerTable} AS {typeof(T2).Name} ON {typeof(TEntity).Name}.{innKey} = {typeof(T2).Name}.{outKey}");

            return new JoinStatement<T2>(StringQuery, Model);
        }

        public JoinStatement<T2> LeftJoin<T2>(Expression<Func<TEntity, object>> innerKey, Expression<Func<T2, object>> outerKey) where T2 : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            StringQuery.AppendLine(
                $"LEFT JOIN dbo.{typeof(T2).Name} AS {typeof(T2).Name} ON {typeof(TEntity).Name}.{innKey} = {typeof(T2).Name}.{outKey}");

            return new JoinStatement<T2>(StringQuery, Model);
        }

        public JoinStatement<T2> LeftJoin<T2>(Expression<Func<TEntity, object>> innerKey, Expression<Func<T2, object>> outerKey, string outerTable) where T2 : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            StringQuery.AppendLine(
                $"LEFT JOIN {outerTable} AS {typeof(T2).Name} ON {typeof(TEntity).Name}.{innKey} = {typeof(T2).Name}.{outKey}");

            return new JoinStatement<T2>(StringQuery, Model);
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
            StringQuery.Append($" WHERE {prop} LIKE @{prop} ");
            ModelValues[prop] = "%" + ModelValues[prop] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the not in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereNotIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} NOT IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public WhereStatement<TEntity, TModel> Where<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {typeof(TEntity).Name}.{propName} = @{paramName}");
            return new WhereStatement<TEntity, TModel>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public WhereStatement<TEntity, TModel> WhereLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {typeof(TEntity).Name}.{propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new WhereStatement<TEntity, TModel>(StringQuery, Model);
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
            StringQuery.Append($" WHERE {prop} LIKE @{prop} ");
            ModelValues[prop] = "%" + ModelValues[prop] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the not in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereNotIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} NOT IN @{valueName}");
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

        /// <summary>
        /// Wheres the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> Where<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} = @{paramName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
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
        public WhereStatement<TEntity> Where(string propName)
        {
            StringQuery.Append($" WHERE {propName} = @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> Where(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} = @{propName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(string propName)
        {
            StringQuery.Append($" WHERE {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the not in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public WhereStatement<TEntity> WhereNotIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} NOT IN @{valueName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> Where<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} = @{paramName}");
            return new WhereStatement<TEntity>(StringQuery, Model);
        }

        public WhereStatement<TEntity> WhereLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {

            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" WHERE {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new WhereStatement<TEntity>(StringQuery, Model);
        }
    }

    public class WhereStatement<T1, T2> : BaseSqlStatement where T1 : class where T2 : class
    {
        public WhereStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }
        public WhereStatement(string query, object model) : base(query, model)
        { }

        public AndStatement<T1, T2> And(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} = @{searchPropName}");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> And<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} = @{searchPropName}");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> AndLike(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} LIKE @{searchPropName}");
            ModelValues[propName] = "%" + ModelValues[searchPropName] + "%";
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> AndLike<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} LIKE @{searchPropName}");
            ModelValues[propName] = "%" + ModelValues[searchPropName] + "%";
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public OrderByStatement<T1> OrderBy(string propName)
        {
            StringQuery.Append($" ORDER BY {propName}");
            return new OrderByStatement<T1>(StringQuery, Model);
        }

        public OrderByStatement<T1> OrderBy(Expression<Func<T1, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" ORDER BY {prop}");
            return new OrderByStatement<T1>(StringQuery, Model);
        }

        public OrderByDescendingStatement<T1> OrderByDescending(string propName)
        {
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<T1>(StringQuery, Model);
        }

        public OrderByDescendingStatement<T1> OrderByDescending(Expression<Func<T1, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<T1>(StringQuery, Model);
        }

        public GroupStatement<T1> GroupBy(Expression<Func<T1, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<T1>(StringQuery, Model);
        }
    }


    public class AndStatement<T1, T2> : BaseSqlStatement where T1 : class where T2 : class
    {
        public AndStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }

        public AndStatement(string query, object model) : base(query, model)
        { }

        public AndStatement<T1, T2> And(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} = @{searchPropName}");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }
        public AndStatement<T1, T2> And(Expression<Func<T1, object>> param, string searchProp)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} = '{searchProp}'");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }
        public AndStatement<T1, T2> And<T3>(Expression<Func<T1, object>> param, string searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} = '{searchProp}'");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }
        public AndStatement<T1, T2> And<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} = @{searchPropName}");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> AndLike(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[searchPropName] + "%";
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> AndLike<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[searchPropName] + "%";
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public OrStatement<T1, T2> Or(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T1).Name}.{propName} = @{searchPropName}");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }
        public OrStatement<T1, T2> Or(Expression<Func<T1, object>> param, string searchProp)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T1).Name}.{propName} = '{searchProp}'");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }
        public OrStatement<T1, T2> Or<T3>(Expression<Func<T3, object>> param, string searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T3).Name}.{propName} = '{searchProp}'");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }
        public OrStatement<T1, T2> Or<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T3).Name}.{propName} = @{searchPropName}");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }

        public OrStatement<T1, T2> OrLike(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T1).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<T1, T2>(StringQuery, Model);
        }

        public OrStatement<T1, T2> OrLike<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T3).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<T1, T2>(StringQuery, Model);
        }

        public OrderByStatement<T1> OrderBy(string propName)
        {
            StringQuery.Append($" ORDER BY {propName}");
            return new OrderByStatement<T1>(StringQuery, Model);
        }

        public OrderByDescendingStatement<T1> OrderByDescending(string propName)
        {
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<T1>(StringQuery, Model);
        }

        public GroupStatement<T1> GroupBy(Expression<Func<T1, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<T1>(StringQuery, Model);
        }

    }

    public class OrStatement<T1, T2> : BaseSqlStatement where T1 : class where T2 : class
    {
        public OrStatement(StringBuilder strBuilder, object model) : base(strBuilder, model)
        { }

        public OrStatement(string query, object model) : base(query, model)
        { }
        public AndStatement<T1, T2> And(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} = @{searchPropName}");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> And(Expression<Func<T1, object>> param, string searchProp)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} = '{searchProp}'");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> And<T3>(Expression<Func<T1, object>> param, string searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} = '{searchProp}'");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> And<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} = @{searchPropName}");
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> AndLike(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T1).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[searchPropName] + "%";
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public AndStatement<T1, T2> AndLike<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" AND {typeof(T3).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[searchPropName] + "%";
            return new AndStatement<T1, T2>(StringQuery, Model);
        }

        public OrStatement<T1, T2> Or(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T1).Name}.{propName} = @{searchPropName}");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }
        public OrStatement<T1, T2> Or(Expression<Func<T1, object>> param, string searchProp)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T1).Name}.{propName} = '{searchProp}'");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }
        public OrStatement<T1, T2> Or<T3>(Expression<Func<T3, object>> param, string searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T3).Name}.{propName} = '{searchProp}'");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }
        public OrStatement<T1, T2> Or<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T3).Name}.{propName} = @{searchPropName}");
            return new OrStatement<T1, T2>(StringQuery, Model);
        }

        public OrStatement<T1, T2> OrLike(Expression<Func<T1, object>> param, Expression<Func<T2, object>> searchProp)
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T1).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<T1, T2>(StringQuery, Model);
        }

        public OrStatement<T1, T2> OrLike<T3>(Expression<Func<T3, object>> param, Expression<Func<T2, object>> searchProp) where T3 : class
        {
            var propName = param.GetExpressionParameter();
            var searchPropName = searchProp.GetExpressionParameter();
            StringQuery.Append($" OR {typeof(T3).Name}.{propName} LIKE @{searchPropName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<T1, T2>(StringQuery, Model);
        }

        public OrderByStatement<T1> OrderBy(string propName)
        {
            StringQuery.Append($" ORDER BY {propName}");
            return new OrderByStatement<T1>(StringQuery, Model);
        }

        public OrderByDescendingStatement<T1> OrderByDescending(string propName)
        {
            StringQuery.Append($" ORDER BY {propName} DESC");
            return new OrderByDescendingStatement<T1>(StringQuery, Model);
        }

        public GroupStatement<T1> GroupBy(Expression<Func<T1, object>> param)
        {
            var prop = param.GetExpressionParameter();
            StringQuery.Append($" GROUP BY {prop}");
            return new GroupStatement<T1>(StringQuery, Model);
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
            StringQuery.Append($" AND {propName} LIKE @{propName}");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
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
            StringQuery.Append($" AND {propName} LIKE @{propName}");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE @{propName}");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE @{propName}");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
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

        /// <summary>
        /// Ands the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> And<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{paramName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ands the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> AndLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE @{paramName}");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> Or<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} = @{paramName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> OrLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} LIKE @{paramName}");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";

            return new OrStatement<TEntity>(StringQuery, Model);
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
            StringQuery.Append($" AND {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
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
            StringQuery.Append($" AND {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
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


        /// <summary>
        /// Ands the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> And<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{paramName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ands the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> AndLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> Or<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} = @{paramName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> OrLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
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
            StringQuery.Append($" AND {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
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
            StringQuery.Append($" AND {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ands the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> And<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{paramName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ands the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> AndLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> Or<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} = @{paramName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> OrLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
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

        public HavingStatement<TEntity> Having(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"HAVING {propName} = @{propName}");
            return new HavingStatement<TEntity>(StringQuery, Model);
        }

        public HavingStatement<TEntity> HavingLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"HAVING LIKE {propName} = @{propName}");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new HavingStatement<TEntity>(StringQuery, Model);
        }

        public HavingStatement<TEntity> HavingCountBigger(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"HAVING COUNT > @{propName}");
            return new HavingStatement<TEntity>(StringQuery, Model);
        }

        public HavingStatement<TEntity> HavingCountLess(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"HAVING COUNT < @{propName}");
            return new HavingStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public HavingStatement<TEntity> HavingIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"HAVING {propName} IN @{valueName}");
            return new HavingStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Wheres the not in.
        /// </summary>
        /// <param name="param">The parameter.</param>
        /// <param name="valueName">Name of the value.</param>
        /// <returns></returns>
        public HavingStatement<TEntity> HavingNotIn(Expression<Func<TEntity, object>> param, string valueName)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($"HAVING {propName} NOT IN @{valueName}");
            return new HavingStatement<TEntity>(StringQuery, Model);
        }
    }

    public class HavingStatement<TEntity> : BaseSqlStatement where TEntity : class
    {
        public HavingStatement(StringBuilder sb) : base(sb)
        { }

        public HavingStatement(StringBuilder sb, object model) : base(sb, model)
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
            StringQuery.Append($" AND {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(string propName)
        {
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(string propName)
        {
            StringQuery.Append($" OR {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> Or(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} = @{propName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        public OrStatement<TEntity> OrLike(Expression<Func<TEntity, object>> param)
        {
            var propName = param.GetExpressionParameter();
            StringQuery.Append($" OR {propName} LIKE @{propName} ");
            ModelValues[propName] = "%" + ModelValues[propName] + "%";
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

        /// <summary>
        /// Ands the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> And<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} = @{paramName}");
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ands the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public AndStatement<TEntity> AndLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" AND {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new AndStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the specified property.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> Or<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} = @{paramName}");
            return new OrStatement<TEntity>(StringQuery, Model);
        }

        /// <summary>
        /// Ors the like.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="prop">The property.</param>
        /// <param name="param">The parameter.</param>
        /// <returns></returns>
        public OrStatement<TEntity> OrLike<TModel>(Expression<Func<TEntity, object>> prop,
            Expression<Func<TModel, object>> param) where TModel : class
        {
            var propName = prop.GetExpressionParameter();
            var paramName = param.GetExpressionParameter();
            StringQuery.Append($" Or {propName} LIKE @{paramName} ");
            ModelValues[paramName] = "%" + ModelValues[paramName] + "%";
            return new OrStatement<TEntity>(StringQuery, Model);
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