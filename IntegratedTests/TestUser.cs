using System;
using System.Collections.Generic;
using System.Text;

namespace IntegratedTests
{
    public class TestUser
    {
        public TestUser()
        {

        }

        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class TestRelationship
    {
        public int Id { get; set; }
        public int UserId { get; set; }
        public string Name { get; set; }
    }

    public class JoinedClass
    {
        public int TestUserId { get; set; }
        public int TestRelationshipUserId { get; set; }
        public int TestRelationshipId { get; set; }
        public string TestRelationshipName { get; set; }
        public string TestUserName { get; set; }
    }
    public class SearchModel
    {
        public string Name { get; set; }
        public int UserId { get; set; }
    }
}
