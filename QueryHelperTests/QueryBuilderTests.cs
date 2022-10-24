using System;
using NUnit.Framework;
using QueryHelper;

namespace QueryHelperTests
{
    [TestFixture]
    public class QueryBuilderTests
    {
        public MockClass MockClass { get; private set; }

        [SetUp]
        public void Setup()
        {
            MockClass = new MockClass();
        }

        [Test]
        public void InsertStatement_IsNot_Null()
        {
            var insertStatement = typeof(MockClass).Insert();
            Assert.IsNotNull(insertStatement);
        }

        [Test]
        public void InsertStatement_Contains_INSERT()
        {
            var insertStatement = typeof(MockClass).Insert();
            Assert.That(insertStatement.Query.Contains("INSERT INTO"));
            Assert.IsNull(insertStatement.Model);
        }

        [Test]
        public void InsertStatement_With_Model_model_Is_Not_Null()
        {
            var insertStatement = typeof(MockClass).Insert(new MockClass());
            Assert.That(insertStatement.Query.Contains("dbo.MockClass"));
            Assert.IsNotNull(insertStatement.Model);
        }

        [Test]
        public void InsertStatement_With_tableName_Contains_tableName()
        {
            var insertStatement = typeof(MockClass).Insert("dbo.User", new MockClass());
            Assert.That(insertStatement.Query.Contains("dbo.User"));
            Assert.IsNotNull(insertStatement.Model);
        }

        [Test]
        public void DeleteStatement_ISNotNull()
        {
            var deleteStatement = MockClass.DeleteStatement(x => x.LastName);
            Assert.IsNotNull(deleteStatement);
        }

        [Test]
        public void SelectStatement_IsNotNull()
        {
            var selectStatement = MockClass.Select();
            Assert.IsNotNull(selectStatement);
        }

        [Test]
        public void SelectStatement_Model_IsNotNull()
        {
            var selectStatement = MockClass.Select();
            Assert.IsNull(selectStatement.Model);
        }
        [Test]
        public void SelectStatement_Query_HasSelect()
        {
            var selectStatement = MockClass.Select();
            Assert.That(selectStatement.Query.Contains("SELECT"));
        }

        [Test]
        public void DeleteStatement_Can_Chain_WithWhere()
        {
            var deleteStatement = MockClass.DeleteStatement().WhereFor(x => x.Name).AndLike(x => x.UserName);
            Assert.IsNotNull(deleteStatement);
            Assert.That(deleteStatement.Query.Contains("WHERE"));
            Assert.That(deleteStatement.Query.Contains("AND"));
        }

        [Test]
        public void DeleteStatement_From_Type()
        {
            var deleteStatement = typeof(MockClass).Delete<MockClass>();
            Assert.IsNull(deleteStatement.Model);
            Assert.IsNotNull(deleteStatement.Query);
        }

        [Test]
        public void DeleteStatementWith_DifferentTableNAme()
        {
            var deleteStatement = typeof(MockClass).Delete<MockClass>("dbo.Test");
            Assert.IsNull(deleteStatement.Model);
            Assert.That(deleteStatement.Query.Contains("dbo.Test"));
        }

        [Test]
        public void DeleteStatementWith_CanPass_the_Model()
        {
            var deleteStatement = typeof(MockClass).Delete<MockClass>("dbo.Test", new MockClass());
            Assert.IsNotNull(deleteStatement);
            Assert.That(deleteStatement.Query.Contains("dbo.Test"));
        }

        [Test]
        public void YouCanChainSelectStatement_WithWhere()
        {
            var statement = MockClass.Select().Where(x => x.LastName);
            Assert.IsNotNull(statement);
            Assert.That(statement.Query.Contains("WHERE"));
        }

        [Test]
        public void SelectStatementChainedWithWhereLike_ContainsLike()
        {
            var statement = MockClass.Select().WhereLike(x => x.UserName);
            Assert.IsNull(statement.Model);
            Assert.That(statement.Query.Contains("LIKE"));
        }

        [Test]
        public void UpdateStatement_On_Type_IsNot_Null()
        {
            var up = typeof(MockClass).Update<MockClass>();
            Assert.IsNotNull(up);
            Assert.IsNotNull(up.Query);
            Assert.IsNull(up.Model);
            Assert.That(up.Query.Contains("UPDATE"));
        }

        [Test]
        public void UpdateStatement_On_Type_With_DifferentTableName_IsNot_Null_ContainsTableName()
        {
            var up = typeof(MockClass).Update<MockClass>("dbo.Table");
            Assert.IsNotNull(up);
            Assert.IsNotNull(up.Query);
            Assert.IsNull(up.Model);
            Assert.That(up.Query.Contains("UPDATE"));
            Assert.That(up.Query.Contains("dbo.Table"));
        }

        [Test]
        public void UpdateStatement_On_Type_Can_ChainWith_WhereStatement()
        {
            var up = typeof(MockClass).Update<MockClass>("dbo.Table").Where(x => x.Id);
            Assert.IsNotNull(up);
            Assert.IsNotNull(up.Query);
            Assert.IsNull(up.Model);
            Assert.That(up.Query.Contains("UPDATE"));
            Assert.That(up.Query.Contains("dbo.Table"));
            Assert.That(up.Query.Contains("WHERE"));
        }

        [Test]
        public void SelectStatement_On_Type_IsNotNull()
        {
            var select = typeof(MockClass).Select<MockClass>();
            Assert.NotNull(select);
            Assert.Null(select.Model);
            Assert.NotNull(select.Query);
            Assert.That(select.Query.Contains("SELECT"));
        }

        [Test]
        public void SelectStatement_On_Type_With_DifferentTableName_Contains_TableName()
        {
            var select = typeof(MockClass).Select<MockClass>("dbo.User");
            Assert.NotNull(select);
            Assert.Null(select.Model);
            Assert.NotNull(select.Query);
            Assert.That(select.Query.Contains("dbo.User"));
        }

        [Test]
        public void SelectStatement_On_Type_With_Model_The_Model_IsPassed()
        {
            var select = typeof(MockClass).Select<MockClass>("dbo.User", new MockClass());
            Assert.NotNull(select);
            Assert.NotNull(select.Model);
            Assert.NotNull(select.Query);
            Assert.That(select.Query.Contains("dbo.User"));
        }

        [Test]
        public void SelectStatement_Can_BeChainedWith_GroupBy()
        {
            var grouped = typeof(MockClass).Select<MockClass>().GroupBy(x => x.Name);
            Assert.IsNotNull(grouped);
            Assert.IsNull(grouped.Model);
            Assert.That(grouped.Query.Contains("GROUP BY"));
        }

        [Test]
        public void SelectStatement_Can_BeChainedWith_GroupBy_Model_CanBePassed()
        {
            var grouped = typeof(MockClass).Select<MockClass>(new MockClass()).GroupBy(x => x.Name);
            Assert.IsNotNull(grouped);
            Assert.IsNotNull(grouped.Model);
            Assert.That(grouped.Query.Contains("GROUP BY"));
        }


        [Test]
        public void WhereStatement_Can_Be_Chained_With_GroupBy()
        {
            var grouped = typeof(MockClass).Select<MockClass>().Where(x => x.LastName).GroupBy(x => x.UserName);
            Assert.IsNotNull(grouped);
            Assert.IsNull(grouped.Model);
            Assert.That(grouped.Query.Contains("SELECT"));
            Assert.That(grouped.Query.Contains("WHERE"));
            Assert.That(grouped.Query.Contains("GROUP BY"));
        }

        [Test]
        public void AndStatement_Can_Be_Chained_With_GroupBy()
        {
            var grouped = typeof(MockClass).Select<MockClass>(new MockClass()).Where(x => x.LastName)
                .And(x => x.Name).GroupBy(x => x.UserName);

            Assert.IsNotNull(grouped);
            Assert.IsNotNull(grouped.Model);
            Assert.That(grouped.Query.Contains("SELECT"));
            Assert.That(grouped.Query.Contains("WHERE"));
            Assert.That(grouped.Query.Contains("AND"));
            Assert.That(grouped.Query.Contains("GROUP BY"));
        }

        [Test]
        public void OrStatement_Can_BeChained_With_Group_By()
        {
            var grouped = typeof(MockClass).Select<MockClass>("dbo.User", new MockClass()).WhereLike(x => x.LastName)
                .And(x => x.Name).Or(x => x.UserName).GroupBy(x => x.UserName);

            Assert.IsNotNull(grouped);
            Assert.IsNotNull(grouped.Model);
            Assert.That(grouped.Query.Contains("SELECT"));
            Assert.That(grouped.Query.Contains("WHERE"));
            Assert.That(grouped.Query.Contains("AND"));
            Assert.That(grouped.Query.Contains("OR"));
            Assert.That(grouped.Query.Contains("GROUP BY"));
        }

        [Test]
        public void GroupBy_Can_Chained_With_OrderBy()
        {
            var order = typeof(MockClass).Select<MockClass>(new MockClass()).GroupBy(x => x.UserName)
                .OrderBy(x => x.Id);
            Assert.IsNotNull(order);
            Assert.IsNotNull(order.Model);
            Assert.That(order.Query.Contains("SELECT"));
            Assert.That(order.Query.Contains("GROUP BY"));
            Assert.That(order.Query.Contains("ORDER"));
        }

        [Test]
        public void GroupBy_Can_Chained_With_OrderByDescending()
        {
            var order = typeof(MockClass).Select<MockClass>(new MockClass()).GroupBy(x => x.UserName)
                .OrderByDescending(x => x.Id);
            Assert.IsNotNull(order);
            Assert.IsNotNull(order.Model);
            Assert.That(order.Query.Contains("SELECT"));
            Assert.That(order.Query.Contains("GROUP BY"));
            Assert.That(order.Query.Contains("ORDER"));
            Assert.That(order.Query.Contains("DESC"));
        }

        [Test]
        public void SelectDistinct_Is_Not_Null()
        {
            var distinct = typeof(MockClass).SelectDistinct<MockClass>(x => x.UserName, x => x.Name);
            Assert.IsNotNull(distinct);
            Assert.IsNull(distinct.Model);
            Assert.That(distinct.Query.Contains("SELECT"));
            Assert.That(distinct.Query.Contains("DISTINCT"));
        }

        [Test]
        public void SelectDistinct_Can_Be_Linked_With_WHERE()
        {
            var where = typeof(MockClass).SelectDistinct<MockClass>(x => x.UserName, x => x.Name, x => x.LastName)
                .Where(x => x.Name);
            Assert.IsNotNull(where);
            Assert.IsNull(where.Model);
            Assert.That(where.Query.Contains("SELECT"));
            Assert.That(where.Query.Contains("DISTINCT"));
            Assert.That(where.Query.Contains("WHERE"));
        }
        [Test]
        public void SelectDistinct_Can_Be_Linked_With_WHERE_LIKE()
        {
            var where = typeof(MockClass).SelectDistinct<MockClass>(x => x.UserName, x => x.Name, x => x.LastName)
                .WhereLike(x => x.Name);
            Assert.IsNotNull(where);
            Assert.IsNull(where.Model);
            Assert.That(where.Query.Contains("SELECT"));
            Assert.That(where.Query.Contains("DISTINCT"));
            Assert.That(where.Query.Contains("WHERE"));
            Assert.That(where.Query.Contains("LIKE"));
        }

        [Test]
        public void Select_Distinct_Can_Be_Linked_With_GROUP_BY()
        {
            var groupBy = typeof(MockClass).SelectDistinct<MockClass>(x => x.UserName, x => x.Name, x => x.LastName)
                .GroupBy(x => x.Name);
            Assert.IsNotNull(groupBy);
            Assert.IsNull(groupBy.Model);
            Assert.That(groupBy.Query.Contains("SELECT"));
            Assert.That(groupBy.Query.Contains("DISTINCT"));
            Assert.That(groupBy.Query.Contains("GROUP BY"));

        }

        [Test]
        public void Select_Distinct_GroupBy_Throws_Argument_Exception_If_The_Property_IS_Not_In_The_SELECT()
        {
            try
            {
                typeof(MockClass).SelectDistinct<MockClass>(x => x.LastName, x => x.Name)
                    .GroupBy(x => x.UserName);
            }
            catch (ArgumentException exc)
            {
                Assert.That(exc.Message.Contains("SELECT"));
            }
        }
        
        [Test]
        public void Join_Table_With_Another_Is_NotNull()
        {
            var joined = MockClass.Join<MockClass, OtherMock>(x => x.Id, x => x.MockId);
            Assert.IsNotNull(joined);
            Assert.IsNotNull(joined.Query);
        }

        [Test]
        public void JoinOn_Table_With_Another_WithTableSpecified_Is_NotNull()
        {
            var joined = MockClass.JoinOn<MockClass, OtherMock>("dbo.Mock",x=>x.Id,x=>x.MockId);
            Assert.IsNotNull(joined);
            Assert.IsNotNull(joined.Query);
        }

        [Test]
        public void JoinOn_Table_With_Another_Is_NotNull()
        {
            var joined = MockClass.JoinOn<MockClass, OtherMock>( x => x.Id, x => x.MockId);
            Assert.IsNotNull(joined);
            Assert.IsNotNull(joined.Query);
        }


        [Test]
        public void Where_With_TwoGenerics_Is_NotNull()
        {
            var searchModel = new SearchModel()
            {
                MockId = 1
            };
            var select = MockClass.Select(searchModel).Where<SearchModel>(x => x.Id, x => x.MockId);
            Assert.IsNotNull(select);
            Assert.IsNotNull(select.Query);
            Assert.That(select.Query.Contains("MockId"));
        }

        [Test]
        public void AndLike_Is_NotNull()
        {
            var searchModel = new SearchModel()
            {
                MockId = 1,
                OtherMockName ="someName"
            };
            var select =MockClass.Select(searchModel).Where<SearchModel>(x => x.Id, x => x.MockId == 1).AndLike<SearchModel>(x => x.Name, x => x.OtherMockName);
            Assert.IsNotNull(select);
        }

    }
}