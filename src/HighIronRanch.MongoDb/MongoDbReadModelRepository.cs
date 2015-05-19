using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
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
			return type.ToString();
		}

		protected MongoCollection<T> GetCollection<T>() where T : IReadModel
		{
			var collectionName = GetCollectionName(typeof (T));
			return GetDatabase().GetCollection<T>(collectionName);
		}

		public async Task<IQueryable<T>> GetAsync<T>() where T : IReadModel, new()
		{
			IQueryable<T> results = null;

			await Task.Run(() => results = Get<T>()).ConfigureAwait(false);
			return results;
		}

		public IQueryable<T> Get<T>() where T : IReadModel, new()
		{
			return RunRetriable(() => (((MongoCollection) GetCollection<T>()).AsQueryable<T>()));
		}

		public async Task<IQueryable<object>> GetAsync(Type type)
		{
			IQueryable<object> results = null;

			await Task.Run(() => results = Get(type)).ConfigureAwait(false);
			return results;
		}

		public IQueryable<object> Get(Type type)
		{
			return RunRetriable(() => GetDatabase().GetCollection(GetCollectionName(type)).AsQueryable());
		}

		public async Task<T> GetByIdAsync<T>(Guid id) where T : IReadModel, new()
		{
			var result = default(T);

			await Task.Run(() => result = GetById<T>(id)).ConfigureAwait(false);

			return result;
		}

		public T GetById<T>(Guid id) where T : IReadModel, new()
		{
			var query = Query.EQ("_id", id);

			return GetCollection<T>().FindOne(query);
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
