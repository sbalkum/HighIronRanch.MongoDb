﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using developwithpassion.specifications.extensions;
using developwithpassion.specifications.rhinomocks;
using HighIronRanch.Core;
using HighIronRanch.Core.Services;
using Machine.Specifications;
using MongoDB.Bson;
using MongoDB.Driver;

namespace HighIronRanch.MongoDb.Test.Integration
{
	public class MongoDbWriteableReadModelRepositorySpecs
    {
		protected const string DatabaseName = "HighIronRanchMongoDbIntegrationTest";
		protected const string connectionStringFile = "C:\\temp\\HighIronRanch.MongoDb.Test.ConnectionString.txt";

	    public class CollectionNamer : ICollectionNamer
	    {
	        public string GetCollectionName(Type type)
	        {
	            return type.ToString();
	        }
	    }

		[Subject(typeof(TestableMongoDbWritableReadModelRepository))]
	    public class Concern : Observes<TestableMongoDbWritableReadModelRepository>
	    {
		    protected static IMongoDbReadModelSettings settings;

            protected static void CreateSettings(string connectionStringPrefix, string connectionStringPostfix)
			{
				if (!File.Exists(connectionStringFile))
				{
					throw new Exception(connectionStringFile + " is missing. This file should contain a single line, probably something like: mongodb://localhost/");
				}
				var connectionString = File.ReadAllText(connectionStringFile);

				settings = depends.@on<IMongoDbReadModelSettings>();
				settings.setup(s => s.MongoDbReadModelConnectionString).Return(connectionStringPrefix + connectionString + connectionStringPostfix);
				settings.setup(s => s.MongoDbReadModelDatabase).Return(DatabaseName);

                var collectionNamer = new CollectionNamer();
			    depends.on<ICollectionNamer>(collectionNamer);
			}
	    }

		public class CleaningConcern : Concern
		{
			private Cleanup after = () =>
			{
				sut.DropDatabase(DatabaseName);
			};
		}

		public class SimpleConcern : CleaningConcern
		{
			protected static TestModel model;
			protected static TestModel modelRead;

			private Establish simpleContext = () =>
			{
				CreateSettings(string.Empty, string.Empty);

				model = TestModel.NewTestModel();
			};
		}

		public class When_writing_to_a_collection : SimpleConcern
	    {
		    private Because of = () =>
		    {
			    sut.Save(model);
			    var models = sut.Get<TestModel>().ToList();
			    var task = sut.GetAsync<TestModel>();
			    var models2 = task.Result.ToList();
				modelRead = sut.GetById<TestModel>(model.Id);
			};

			private It should_read_the_correct_model_id = () => modelRead.Id.ShouldEqual(model.Id);
			private It should_read_the_correct_model_value = () => modelRead.Value.ShouldEqual(model.Value);
	    }

		public class When_writing_several_to_a_collection : SimpleConcern
		{
			protected static List<TestModel> models = new List<TestModel>();
			protected static IEnumerable<TestModel> modelsRead;

			private Establish concernContext = () =>
			{
				for (int i = 0; i < 10; i++)
				{
					models.Add(TestModel.NewTestModel());
				}
			};

			private Because of = () =>
			{
				sut.Insert(models);
				modelsRead = sut.Get<TestModel>();
			};

			private It should_read_all_models = () =>
			{
				var dict = new Dictionary<Guid, string>();

				foreach (var m in modelsRead)
				{
					models.Contains(m).ShouldEqual(true);
					dict.Add(m.Id, m.Value);
				}
				dict.Count.ShouldEqual(models.Count);
			};
		}

		public class When_deleting_a_model_from_a_collection : SimpleConcern
	    {
			private Because of = () =>
			{
				sut.Save(model);
				sut.Delete(model);
				modelRead = sut.GetById<TestModel>(model.Id);
			};

			private It should_not_read_the_model = () => modelRead.ShouldBeNull();
		}

		public class When_the_connection_string_is_bad : Concern
		{
			private static TestModel model;

			private static Exception _expectedException;

			private Establish context = () =>
			{
				CreateSettings("x", string.Empty);

				model = TestModel.NewTestModel();
			};

			private Because of = () =>
			{
				_expectedException = Catch.Exception(() => sut.Save(model));
			};

			private It should_throw_an_exception =
				() => _expectedException.ShouldBeOfExactType(typeof (MongoConfigurationException));
		}

		public class When_the_socket_is_really_slow : CleaningConcern
		{
			private static IList<TestModel> models;

			private static Exception _expectedException;

			private Establish context = () =>
			{
				CreateSettings(string.Empty, "?socketTimeoutMS=1000");

				models = new List<TestModel>();
				for (int i = 0; i < 10; i++)
				{
					models.Add(TestModel.NewTestModel());
				}
			};

			private Because of = () =>
			{
				sut.Insert(models);
				_expectedException = Catch.Exception(() => sut.GetSlowly<TestModel>());
			};

			private It should_throw_an_exception =
				() => _expectedException.ShouldBeOfExactType(typeof(MongoConnectionException));
		}

		public class When_the_socket_is_retried : CleaningConcern
		{
			private static IList<TestModel> models;
			private static string result;

			private Establish context = () =>
			{
				CreateSettings(string.Empty, string.Empty);

				models = new List<TestModel>();
				for (int i = 0; i < 2; i++)
				{
					models.Add(TestModel.NewTestModel());
				}
			};

			private Because of = () =>
			{
				sut.Insert(models);
				result = sut.GetFasterWithEachTry<TestModel>();
			};

			private It should_get_the_results = () => string.IsNullOrEmpty(result).ShouldEqual(false);
		}

	}

	public class TestableMongoDbWritableReadModelRepository : MongoDbWritableReadModelRepository
	{
		public TestableMongoDbWritableReadModelRepository(IMongoDbReadModelSettings settings, ICollectionNamer collectionNamer) : base(settings, collectionNamer)
		{
		}

		public void DropDatabase(string name)
		{
			GetServer().DropDatabase(name);
		}

		public string GetSlowly<T>() where T : IReadModel, new()
		{
			var collectionName = GetCollectionName(typeof (T));
			var database = GetDatabase();
			var result = database.Eval(new EvalArgs()
			{
				Code = "function() { return db." + collectionName + ".find({$where: 'sleep(1000) || true'}).toArray(); }"
			});
			return result.ToJson();
		}

		public string GetFasterWithEachTry<T>() where T : IReadModel, new()
		{
			var startingConnectionString = _connectionString;

			for (int i = 0; i < 3; i++)
			{
				_connectionString = startingConnectionString + "?socketTimeoutMS=" + (1000 + (i*15000));
				try
				{
					return GetSlowly<T>();
				}
				catch (MongoConnectionException)
                {
				}
			}

			throw new Exception("Could not get results in 3 tries.");
		}
	}

	public class TestModel : IReadModel
	{
		public Guid Id { get; set; }
		public String Value;

		public static TestModel NewTestModel()
		{
			return new TestModel()
			{
				Id = Guid.NewGuid(),
				Value = DateTime.Now.ToLongTimeString()
			};
		}

		public override bool Equals(object obj)
		{
			var m = obj as TestModel;

			if (m == null)
				return false;

			return m.Id == Id && m.Value == Value;
		}

		public override int GetHashCode()
		{
			return Id.GetHashCode();
		}
	}

}
