using System;
using System.Collections.Generic;
using System.Linq;
using HighIronRanch.Core;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;

namespace HighIronRanch.MongoDb
{
	public class MongoDbReadModelRepository : IReadModelRepository
	{
		private readonly string _connectionString;
		private readonly string _databaseName;

		public MongoDbReadModelRepository(IMongoDbReadModelSettings settings)
		{
			_connectionString = settings.MongoDbReadModelConnectionString;
			_databaseName = settings.MongoDbReadModelDatabase;
		}

		// public for integration tests
		public MongoServer GetServer()
		{
			var client = new MongoClient(_connectionString);
			return client.GetServer();
		}

		public MongoDatabase GetDatabase()
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
			return (((MongoCollection)GetCollection<T>()).AsQueryable<T>());
		}

		public IQueryable<object> Get(Type type)
		{
			var collectionName = GetCollectionName(type);
			var collection = GetDatabase().GetCollection(collectionName);
			return collection.AsQueryable();
		}

		public T GetById<T>(Guid id) where T : IReadModel, new()
		{
			var collection = GetCollection<T>();
			var query = Query.EQ("_id", id);
			return (collection.FindOne(query));
		}
	}
}
