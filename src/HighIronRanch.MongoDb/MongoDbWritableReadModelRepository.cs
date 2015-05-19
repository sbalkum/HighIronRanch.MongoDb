using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HighIronRanch.Core;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace HighIronRanch.MongoDb
{
	public class MongoDbWritableReadModelRepository : MongoDbReadModelRepository, IWritableReadModelRepository
	{
		public MongoDbWritableReadModelRepository(IMongoDbReadModelSettings settings) : base(settings)
		{
		}

		public async Task TruncateAsync<T>() where T : IReadModel
		{
			await Task.Run(() => Truncate<T>()).ConfigureAwait(false);
		}

		public void Truncate<T>() where T : IReadModel
		{
			RunRetriable(() => GetDatabase().DropCollection(GetCollectionName(typeof (T))));
		}

		public async Task SaveAsync<T>(T item) where T : IReadModel
		{
			await Task.Run(() => Save<T>(item)).ConfigureAwait(false);
		}

		public void Save<T>(T item) where T : IReadModel
		{
			RunRetriable(() => GetCollection<T>().Save(item));
		}

		public async Task DeleteAsync<T>(T item) where T : IReadModel
		{
			await Task.Run(() => Delete<T>(item)).ConfigureAwait(false);
		}

		public void Delete<T>(T item) where T : IReadModel
		{
			var query = Query.EQ("_id", BsonValue.Create(item.Id));
			RunRetriable(() => GetCollection<T>().Remove(query));
		}

		public async Task InsertAsync<T>(IEnumerable<T> items) where T : IReadModel
		{
			await Task.Run(() => Insert<T>(items)).ConfigureAwait(false);
		}

		public void Insert<T>(IEnumerable<T> items) where T : IReadModel
		{
			RunRetriable(() => GetCollection<T>().InsertBatch(items));
		}

		public void BulkSetProperty<T>(IEnumerable<Guid> ids, string propertyName, object value) where T : IReadModel
		{
			var bsonIds = ids.Select(i => BsonValue.Create(i));
			var query = Query.In("_id", bsonIds);
			var updatebuilder = new UpdateBuilder();
			updatebuilder.Set(propertyName, BsonValue.Create(value));

			RunRetriable(() =>
			{
				var collection = GetCollection<T>();
				var bulk = collection.InitializeUnorderedBulkOperation();
				bulk.Find(query).Update(updatebuilder);

				return bulk.Execute();
			});
		}

		public long RenameField<T>(string oldField, string newField) where T : IReadModel
		{
			var rename = Update.Rename(oldField, newField);
			var query = Query.Exists(oldField);

			return RunRetriable(() =>
			{
				var result = GetCollection<T>().Update(query, rename, UpdateFlags.Multi);
				return result.DocumentsAffected;
			});
		}
	}
}