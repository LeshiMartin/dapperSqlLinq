using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace QueryHelper
{
    public static class Extensions
    {
        public static string GetExpressionParameter<TEntity>(this Expression<Func<TEntity, object>> Where)
        {
            if (Where.Body is MemberExpression member)
                return member.Member.Name;
            return Where.Compile()
                .ToString();
        }
    }
}
