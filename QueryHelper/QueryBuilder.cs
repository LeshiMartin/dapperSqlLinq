using System;
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
            var deleteInit = new StringBuilder($"DELETE * FROM dbo.{typeof(TEntity).Name}");
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
            var deleteSt = new StringBuilder($"DELETE * FROM dbo.{typeof(TEntity).Name}");
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
        /// Updates the specified type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static UpdateStatement<TEntity> Update<TEntity>(this Type type) where TEntity : class
        {
            var query = $"UPDATE dbo.{type.Name} SET ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
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
            var query = $"UPDATE dbo.{type.Name} SET ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
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
            var query = $"UPDATE {tableName} SET ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
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
            var query = $"UPDATE {tableName} SET ";
            var properties = typeof(TEntity).GetProperties();
            var updatePropSet = properties.Aggregate(string.Empty, (str, prop) =>
            {
                str = string.IsNullOrEmpty(str) ? $"{prop.Name} = @{prop.Name}" : $"{str}, {prop.Name} = @{prop.Name}";
                return str;
            });
            return new UpdateStatement<TEntity>(new StringBuilder(string.Format(query, updatePropSet)), model);
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
        /// Selects the specified entity.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public static SelectStatement<TEntity> Select<TEntity>(this TEntity entity) where TEntity : class
        {
            var selectStatement = new StringBuilder($"SELECT * FROM dbo.{typeof(TEntity).Name}");
            return new SelectStatement<TEntity>(selectStatement, entity);
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
            return new SelectStatement<TEntity>(selectStatement, entity);
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
            var selectStatement = new StringBuilder($"SELECT * FROM dbo.{type.Name}");
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

        public static SelectDistinctStatement<TEntity> SelectDistinct<TEntity>(this Type type,
             object model, params Expression<Func<TEntity, object>>[] props) where TEntity : class
        {
            var queryStr = "SELECT DISTINCT {0} from dbo." + type.Name;
            var propNames = props.Aggregate(string.Empty, (str, pr) =>
            {
                var propName = pr.GetExpressionParameter();
                return string.IsNullOrEmpty(str) ? propName : $"{str}, {propName}";
            });

            return new SelectDistinctStatement<TEntity>(new StringBuilder(string.Format(queryStr, propNames)),model);
        }

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
        public static SelectDistinctStatement<TEntity> SelectDistinct<TEntity>(this Type type,
            string tableName,object model ,params Expression<Func<TEntity, object>>[] props) where TEntity : class
        {
            var queryStr = "SELECT DISTINCT {0} from " + tableName;
            var propNames = props.Aggregate(string.Empty, (str, pr) =>
            {
                var propName = pr.GetExpressionParameter();
                return string.IsNullOrEmpty(str) ? propName : $"{str}, {propName}";
            });

            return new SelectDistinctStatement<TEntity>(new StringBuilder(string.Format(queryStr, propNames)),model);
        }

        private static string FillPropertiesAndValues(PropertyInfo[] properties, string insertProperties, ref string values)
        {
            foreach (var propertyInfo in properties)
            {
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
