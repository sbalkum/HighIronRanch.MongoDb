using System;
using System.Collections.Generic;
using System.Linq;
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

		public void Truncate<T>() where T : IReadModel
		{
			RunRetriable(() => GetDatabase().DropCollection(GetCollectionName(typeof(T))));
		}

		public void Save<T>(T item) where T : IReadModel
		{
			RunRetriable(() => GetCollection<T>().Save(item));
		}

		public void Insert<T>(IEnumerable<T> items) where T : IReadModel
		{
			RunRetriable(() => GetCollection<T>().InsertBatch(items));
		}

		public void Delete<T>(T item) where T : IReadModel
		{
			var query = Query.EQ("_id", BsonValue.Create(item.Id));
			RunRetriable(() => GetCollection<T>().Remove(query));
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