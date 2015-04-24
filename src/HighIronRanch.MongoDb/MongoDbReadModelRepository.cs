using System;
using System.IO;
using System.Linq;
using HighIronRanch.Core;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;

namespace HighIronRanch.MongoDb
{
	public class MongoDbReadModelRepository : IReadModelRepository
	{
		protected const int MaxNumberOfAttempts = 3;

		protected string _connectionString;
		protected readonly string _databaseName;

		public MongoDbReadModelRepository(IMongoDbReadModelSettings settings)
		{
			_connectionString = settings.MongoDbReadModelConnectionString;
			_databaseName = settings.MongoDbReadModelDatabase;
		}

		protected MongoServer GetServer()
		{
			var client = new MongoClient(_connectionString);
			return client.GetServer();
		}

		protected MongoDatabase GetDatabase()
		{
			return GetServer().GetDatabase(_databaseName);
		}

		protected string GetCollectionName(Type type)
		{
			return type.ToString().Replace("Portal.Core.", "Portal.");
		}

		protected MongoCollection<T> GetCollection<T>() where T : IReadModel
		{
			var collectionName = GetCollectionName(typeof (T));
			return GetDatabase().GetCollection<T>(collectionName);
		}

		public IQueryable<T> Get<T>() where T : IReadModel, new()
		{
			return RunRetriable(() => (((MongoCollection) GetCollection<T>()).AsQueryable<T>()));
		}

		public IQueryable<object> Get(Type type)
		{
			return RunRetriable(() =>
			{
				var collectionName = GetCollectionName(type);
				var collection = GetDatabase().GetCollection(collectionName);
				return collection.AsQueryable();
			});
		}

		public T GetById<T>(Guid id) where T : IReadModel, new()
		{
			return RunRetriable(() =>
			{
				var collection = GetCollection<T>();
				var query = Query.EQ("_id", id);
				return (collection.FindOne(query));
			});
		}

		protected static T RunRetriable<T>(Func<T> action)
		{
			for (int i = 1; i <= MaxNumberOfAttempts; i++)
			{
				try
				{
					return action();
				}
				catch (IOException)
				{
					if (i == MaxNumberOfAttempts)
						throw;
				}
			}
			throw new Exception("Something went wrong after " + MaxNumberOfAttempts + " attempts");
		}
	}
}
