namespace HighIronRanch.MongoDb
{
	public interface IMongoDbReadModelSettings
	{
		string MongoDbReadModelConnectionString { get; }
		string MongoDbReadModelDatabase { get; }
	}
}