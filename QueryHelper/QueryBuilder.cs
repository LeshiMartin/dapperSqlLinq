using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace QueryHelper
{


    public static class Dslinq
    {
        /// <summary>
        /// Deletes the statement.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="where">The where.</param>
        /// <returns></returns>
        public static BaseSqlStatement DeleteStatement<TEntity>(this TEntity entity, Expression<Func<TEntity, object>> @where) where TEntity : class
        {
            var deleteInit = new StringBuilder($"DELETE * FROM dbo.{typeof(TEntity).GetNameFromType()}");
            return new DeleteStatement<TEntity>(deleteInit).Where(@where.GetExpressionParameter());
        }

        /// <summary>
        /// Deletes the statement.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="where">The where.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static BaseSqlStatement DeleteStatement<TEntity>(this TEntity entity, Expression<Func<TEntity, object>> @where, string tableName) where TEntity : class
        {
            var deleteInit = new StringBuilder($"DELETE * FROM {tableName}");
            return new DeleteStatement<TEntity>(deleteInit).Where(@where.GetExpressionParameter());
        }

        /// <summary>
        /// Deletes the statement.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public static DeleteStatement<TEntity> DeleteStatement<TEntity>(this TEntity entity) where TEntity : class
        {
            var deleteSt = new StringBuilder($"DELETE * FROM dbo.{typeof(TEntity).GetNameFromType()}");
            return new DeleteStatement<TEntity>(deleteSt, entity);
        }

        /// <summary>
        /// Deletes the specified type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static DeleteStatement<TEntity> Delete<TEntity>(this Type type) where TEntity : class
        {
            var deleteSt = new StringBuilder($"DELETE * FROM dbo.{type.Name}");
            return new DeleteStatement<TEntity>(deleteSt);
        }

        /// <summary>
        /// Deletes the specified model.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="model">The model used for passing the value.</param>
        /// <returns></returns>
        public static DeleteStatement<TEntity> Delete<TEntity>(this Type type, object model) where TEntity : class
        {
            var deleteSt = new StringBuilder($"DELETE * FROM dbo.{type.Name}");
            return new DeleteStatement<TEntity>(deleteSt, model);
        }

        /// <summary>
        /// Deletes the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table with schema.</param>
        /// <returns></returns>
        public static DeleteStatement<TEntity> Delete<TEntity>(this Type type, string tableName) where TEntity : class
        {
            var deleteSt = new StringBuilder($"DELETE * FROM {tableName}");
            return new DeleteStatement<TEntity>(deleteSt);
        }

        /// <summary>
        /// Deletes the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static DeleteStatement<TEntity> Delete<TEntity>(this Type type, string tableName, object model) where TEntity : class
        {
            var deleteSt = new StringBuilder($"DELETE * FROM {tableName}");
            return new DeleteStatement<TEntity>(deleteSt, model);
        }

        /// <summary>
        /// Deletes the statement.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="table">The table with schema included.</param>
        /// <returns></returns>
        public static DeleteStatement<TEntity> DeleteStatement<TEntity>(this TEntity entity, string table)
            where TEntity : class
        {
            var deleteSt = new StringBuilder($"DELETE * FROM {table}");
            return new DeleteStatement<TEntity>(deleteSt, entity);
        }

        /// <summary>
        /// Inserts the specified entity.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public static InsertStatement Insert<TEntity>(this TEntity entity) where TEntity : class
        {
            var query = $"INSERT INTO dbo.{typeof(TEntity).GetNameFromType()}" + "{0} {1}";
            var properties = typeof(TEntity).GetProperties();
            var insertProperties = string.Empty;
            var values = string.Empty;
            insertProperties = FillPropertiesAndValues(properties, insertProperties, ref values);

            insertProperties = $"{insertProperties})";
            values = $"{values})";
            return new InsertStatement(string.Format(query, insertProperties, values), entity);
        }

        /// <summary>
        /// Inserts the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static InsertStatement Insert<TEntity>(this TEntity entity, string tableName) where TEntity : class
        {
            var query = $"INSERT INTO {tableName}" + "{0} {1}";
            var properties = typeof(TEntity).GetProperties();
            var insertProperties = string.Empty;
            var values = string.Empty;
            insertProperties = FillPropertiesAndValues(properties, insertProperties, ref values);

            insertProperties = $"{insertProperties})";
            values = $"{values})";
            return new InsertStatement(string.Format(query, insertProperties, values), entity);
        }

        /// <summary>
        /// Inserts the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static InsertStatement Insert(this Type type)
        {
            var query = $"INSERT INTO dbo.{type.Name}" + "{0} {1}";
            var properties = type.GetProperties();
            var insertProperties = string.Empty;
            var values = string.Empty;
            insertProperties = FillPropertiesAndValues(properties, insertProperties, ref values);

            insertProperties = $"{insertProperties})";
            values = $"{values})";
            return new InsertStatement(string.Format(query, insertProperties, values));
        }

        /// <summary>
        /// Inserts the specified table name.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table with schema included.</param>
        /// <returns></returns>
        public static InsertStatement Insert(this Type type, string tableName)
        {
            var query = $"INSERT INTO {tableName}" + "{0} {1}";
            var properties = type.GetProperties();
            var insertProperties = string.Empty;
            var values = string.Empty;
            insertProperties = FillPropertiesAndValues(properties, insertProperties, ref values);

            insertProperties = $"{insertProperties})";
            values = $"{values})";
            return new InsertStatement(string.Format(query, insertProperties, values));
        }

        /// <summary>
        /// Inserts the specified model.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="model">The model used for filling the values.</param>
        /// <returns></returns>
        public static InsertStatement Insert(this Type type, object model)
        {
            var query = $"INSERT INTO dbo.{type.Name}" + "{0} {1}";
            var properties = type.GetProperties();
            var insertProperties = string.Empty;
            var values = string.Empty;
            insertProperties = FillPropertiesAndValues(properties, insertProperties, ref values);

            insertProperties = $"{insertProperties})";
            values = $"{values})";
            return new InsertStatement(string.Format(query, insertProperties, values), model);
        }


        /// <summary>
        /// Inserts the specified table name.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table with schema included.</param>
        /// <param name="model">The model used for filling the values.</param>
        /// <returns></returns>
        public static InsertStatement Insert(this Type type, string tableName, object model)
        {
            var query = $"INSERT INTO {tableName}" + "{0} {1}";
            var properties = type.GetProperties();
            var insertProperties = string.Empty;
            var values = string.Empty;
            insertProperties = FillPropertiesAndValues(properties, insertProperties, ref values);

            insertProperties = $"{insertProperties})";
            values = $"{values})";
            return new InsertStatement(string.Format(query, insertProperties, values), model);
        }

        /// <summary>
        /// Updates the specified type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this TEntity entity) where TEntity : class
        {
            var query = $"UPDATE dbo.{typeof(TEntity).GetNameFromType()} SET {{0}} ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                if (prop.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    return str;
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)), entity);
        }

        /// <summary>
        /// Updates the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity"></param>
        /// <param name="tableName">Name of the table with schema.</param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this TEntity entity, string tableName) where TEntity : class
        {
            var query = $"UPDATE {tableName} SET {{0}} ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                if (prop.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    return str;
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)), entity);
        }

        /// <summary>
        /// Updates the specified type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this Type type) where TEntity : class
        {
            var query = $"UPDATE dbo.{type.Name} SET {{0}} ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                if (prop.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    return str;
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)));
        }

        /// <summary>
        /// Updates the specified model.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="model">The model used for filling the values.</param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this Type type, object model) where TEntity : class
        {
            var query = $"UPDATE dbo.{type.Name} SET {{0}} ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                if (prop.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    return str;
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)), model);
        }

        /// <summary>
        /// Updates the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table with schema.</param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this Type type, string tableName) where TEntity : class
        {
            var query = $"UPDATE {tableName} SET {{0}} ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                if (prop.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    return str;
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)));
        }

        /// <summary>
        /// Updates the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table with schema.</param>
        /// <param name="model">The model used for filling the values.</param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this Type type, string tableName, object model) where TEntity : class
        {
            var query = $"UPDATE {tableName} SET {{0}} ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                if (prop.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    return str;
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)), model);
        }


        /// <summary>
        /// Selects the specified entity.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this TEntity entity) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()}");
            return new SelectStatement<TEntity>(selectStatement);
        }

        public static SelectStatement<TEntity> Select<TEntity>(this TEntity entity, object model) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} ");
            return new SelectStatement<TEntity>(selectStatement, model);
        }

        /// <summary>
        /// Selects the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this TEntity entity, string tableName) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM {tableName}");
            return new SelectStatement<TEntity>(selectStatement);
        }

        /// <summary>
        /// Selects the specified table name.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this TEntity entity, string tableName, object model) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM {tableName}");
            return new SelectStatement<TEntity>(selectStatement, model);
        }

        /// <summary>
        /// Selects the specified type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this Type type) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM dbo.{type.Name}");
            return new SelectStatement<TEntity>(selectStatement);
        }

        /// <summary>
        /// Selects the specified model.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity>
            Select<TEntity>(this Type type, object model) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM dbo.{type.Name} ");
            return new SelectStatement<TEntity>(selectStatement, model);
        }

        /// <summary>
        /// Selects the specified table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="table">The table with schema.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this Type type, string table) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM {table}");
            return new SelectStatement<TEntity>(selectStatement);
        }

        /// <summary>
        /// Selects the specified table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="table">The table.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this Type type, string table, object model) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM {table}");
            return new SelectStatement<TEntity>(selectStatement, model);
        }

        /// <summary>
        /// Selects the distinct.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="props">The props.</param>
        /// <returns></returns>
        public static SelectDistinctStatement<TEntity> SelectDistinct<TEntity>(this Type type, params Expression<Func<TEntity, object>>[] props) where TEntity : class
        {
            var queryStr = "SELECT DISTINCT {0} from dbo." + type.Name;
            var propNames = props.Aggregate(string.Empty, (str, pr) =>
            {
                var propName = pr.GetExpressionParameter();
                return string.IsNullOrEmpty(str) ? propName : $"{str}, {propName}";
            });

            return new SelectDistinctStatement<TEntity>(new StringBuilder(string.Format(queryStr, propNames)));
        }

        /// <summary>
        /// Selects the distinct.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="model">The model.</param>
        /// <param name="props">The props.</param>
        /// <returns></returns>
        public static SelectDistinctStatement<TEntity> SelectDistinct<TEntity>(this Type type,
             object model, params Expression<Func<TEntity, object>>[] props) where TEntity : class
        {
            var queryStr = "SELECT DISTINCT {0} from dbo." + type.Name;
            var propNames = props.Aggregate(string.Empty, (str, pr) =>
            {
                var propName = pr.GetExpressionParameter();
                return string.IsNullOrEmpty(str) ? propName : $"{str}, {propName}";
            });

            return new SelectDistinctStatement<TEntity>(new StringBuilder(string.Format(queryStr, propNames)), model);
        }

        /// <summary>
        /// Selects the distinct.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="props">The props.</param>
        /// <returns></returns>
        public static SelectDistinctStatement<TEntity> SelectDistinct<TEntity>(this Type type,
            string tableName, params Expression<Func<TEntity, object>>[] props) where TEntity : class
        {
            var queryStr = "SELECT DISTINCT {0} from " + tableName;
            var propNames = props.Aggregate(string.Empty, (str, pr) =>
            {
                var propName = pr.GetExpressionParameter();
                return string.IsNullOrEmpty(str) ? propName : $"{str}, {propName}";
            });

            return new SelectDistinctStatement<TEntity>(new StringBuilder(string.Format(queryStr, propNames)));
        }

        /// <summary>
        /// Selects the distinct.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="model">The model.</param>
        /// <param name="props">The props.</param>
        /// <returns></returns>
        public static SelectDistinctStatement<TEntity> SelectDistinct<TEntity>(this Type type,
            string tableName, object model, params Expression<Func<TEntity, object>>[] props) where TEntity : class
        {
            var queryStr = "SELECT DISTINCT {0} from " + tableName;
            var propNames = props.Aggregate(string.Empty, (str, pr) =>
            {
                var propName = pr.GetExpressionParameter();
                return string.IsNullOrEmpty(str) ? propName : $"{str}, {propName}";
            });

            return new SelectDistinctStatement<TEntity>(new StringBuilder(string.Format(queryStr, propNames)), model);
        }

        /// <summary>
        /// Joins the specified inner key.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {

            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }



        /// <summary>
        /// Joins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// Gets the select values.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        public static string[] GetSelectValues<T>(this T t) where T : class
        {
            var properties = t.GetType().GetProperties().Select(x => $"{typeof(T).Name}.{x.Name} AS {typeof(T).Name}{x.Name}").ToArray();
            return properties;
        }

        /// <summary>
        /// Gets the select values.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        public static string[] GetSelectValues(Type t) { 
            var properties = t.GetProperties().Select(x => $"{t.Name}.{x.Name} AS {t.Name}{x.Name}").ToArray();
            return properties;
        }

        /// <summary>
        /// Joins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="selectValues"></param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, IEnumerable<string> selectValues,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT {string.Join(", ",selectValues)} FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, IEnumerable<string> selectValues, string innerTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT {string.Join(", ", selectValues)} FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, IEnumerable<string> selectValues, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT {string.Join(", ", selectValues)} FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN {joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, string innerTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, string innerTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name="joinTable"></param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// Joins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS t");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS j ON t.{innKey} = j.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// Joins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> Join<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, IEnumerable<string> selectValues,
         Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
         where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT {string.Join(", ", selectValues)} FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, IEnumerable<string> selectValues, string innerTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT {string.Join(", ", selectValues)} FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <typeparam name="TModel"></typeparam>
        /// <param name="entity"></param>
        /// <param name="selectValues"></param>
        /// <param name="innerTable"></param>
        /// <param name="joinTable"></param>
        /// <param name="innerKey"></param>
        /// <param name="outerKey"></param>
        /// <param name="model"></param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, IEnumerable<string> selectValues, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();

            var st = new StringBuilder($"SELECT {string.Join(", ", selectValues)} FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN {joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} =  {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, string innerTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, string innerTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{typeof(TModel).GetNameFromType()} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name="joinTable"></param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// Joins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS t");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS j ON t.{innKey} = j.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// Joins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// Joins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> JoinOn<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($" JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }


        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this TEntity entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this TEntity entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this TEntity entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this TEntity entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st, model);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TModel> LeftJoin<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TModel>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this TEntity entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this TEntity entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).GetNameFromType()} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this TEntity entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this TEntity entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this TEntity entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified join table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this Type entity, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM dbo.{entity.Name} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st, model);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this Type entity, string innerTable, Type joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable.Name} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }

        /// <summary>
        /// LeftJoins the specified inner table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="innerTable">The inner table.</param>
        /// <param name=" JOINTable">The join table.</param>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="model">The model.</param>
        /// <returns></returns>
        public static JoinStatement<TEntity> LeftJoinOn<TEntity, TModel>(this Type entity, string innerTable, string joinTable,
            Expression<Func<TEntity, object>> innerKey, Expression<Func<TModel, object>> outerKey, object model) where TEntity : class
            where TModel : class
        {
            var innKey = innerKey.GetExpressionParameter();
            var outKey = outerKey.GetExpressionParameter();
            var st = new StringBuilder($"SELECT * FROM {innerTable} AS {typeof(TEntity).GetNameFromType()}");
            st.AppendLine($"LEFT JOIN dbo.{joinTable} AS {typeof(TModel).GetNameFromType()} ON {typeof(TEntity).GetNameFromType()}.{innKey} = {typeof(TModel).GetNameFromType()}.{outKey}");
            return new JoinStatement<TEntity>(st);
        }


        private static string FillPropertiesAndValues(PropertyInfo[] properties, string insertProperties, ref string values)
        {
            foreach (var propertyInfo in properties)
            {
                if (propertyInfo.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                    continue;

                if (string.IsNullOrEmpty(insertProperties))
                {
                    insertProperties = $"({propertyInfo.Name}";
                    values = $"VALUES(@{propertyInfo.Name}";
                }
                else
                {
                    insertProperties = $"{insertProperties}, {propertyInfo.Name}";
                    values = $"{values}, @{propertyInfo.Name}";
                }
            }

            return insertProperties;
        }

    }
}
