using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace QueryHelper
{
    public static class Extensions
    {
        public static string GetExpressionParameter<TEntity>(this Expression<Func<TEntity, object>> @where)
        {
            switch (@where.Body)
            {
                case MemberExpression member:
                    return member.Member.Name;
                case UnaryExpression unary when unary.Operand is MemberExpression unMem:
                    return unMem.Member.Name;
                default:
                    return @where.Compile()
                        .ToString();
            }
        }

        public static string GetNameFromType(this Type type)
        {
            var name = type.Name;
            if (name.Contains("User"))
                name = $"[{name}]";
            return name;
        }

        public static object ToObject(this IDictionary<string, object> objectValues, object obj)
        {
            
            var objType = obj.GetType();
            foreach (var kvp in objectValues)
            {
                objType.GetProperty(kvp.Key)?.SetValue(obj, kvp.Value,null);
            }

            return obj;
        }

        public static IDictionary<string, object> AsDictionary(this object source, BindingFlags bindingAttr = BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance)
        {
            return source.GetType().GetProperties(bindingAttr).ToDictionary
            (
                propInfo => propInfo.Name,
                propInfo => propInfo.GetValue(source, null)
            );

        }
    }
}
