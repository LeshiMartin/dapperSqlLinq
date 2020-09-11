namespace QueryHelperTests
{
    public class MockClass
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }

    }

    public class OtherMock
    {
        public int Id { get; set; }
        public int MockId { get; set; }
        public string Name { get; set; }
    }

    public class SearchModel
    {
        public int MockId { get; set; }
        public string OtherMockName{ get; set; }
    }
}