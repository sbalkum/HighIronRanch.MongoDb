using System;
using System.Collections.Generic;
using System.IO;
using developwithpassion.specifications.extensions;
using developwithpassion.specifications.rhinomocks;
using Machine.Specifications;
using HighIronRanch.Core;
using MongoDB.Bson;
using MongoDB.Driver;

namespace HighIronRanch.MongoDb.Test.Integration
{
	public class MongoDbWriteableReadModelRepositorySpecs
    {
		protected const string DatabaseName = "HighIronRanchMongoDbIntegrationTest";
		protected const string connectionStringFile = "C:\\temp\\HighIronRanch.MongoDb.Test.ConnectionString.txt";

		[Subject(typeof(TestableMongoDbWritableReadModelRepository))]
	    public class Concern : Observes<TestableMongoDbWritableReadModelRepository>
	    {
		    protected static IMongoDbReadModelSettings settings;

			protected static void CreateSettings(string connectionStringPostfix)
			{
				if (!File.Exists(connectionStringFile))
				{
					throw new Exception(connectionStringFile + " is missing. This file should contain a single line, probably something like: mongodb://localhost/");
				}
				var connectionString = File.ReadAllText(connectionStringFile);

				settings = depends.@on<IMongoDbReadModelSettings>();
				settings.setup(s => s.MongoDbReadModelConnectionString).Return(connectionString + connectionStringPostfix);
				settings.setup(s => s.MongoDbReadModelDatabase).Return(DatabaseName);
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

			private Establish concernContext = () =>
			{
				CreateSettings(string.Empty);

				model = TestModel.NewTestModel();
			};
		}

		public class When_writing_to_a_collection : SimpleConcern
	    {
		    private Because of = () =>
		    {
			    sut.Save(model);
			    modelRead = sut.GetById<TestModel>(model.Id);
		    };

		    private It should_read_the_correct_model_id = () => modelRead.Id.ShouldEqual(model.Id);
			private It should_read_the_correct_model_value = () => modelRead.Value.ShouldEqual(model.Value);
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
				CreateSettings("?garbage=1");

				model = TestModel.NewTestModel();
			};

			private Because of = () =>
			{
				_expectedException = Catch.Exception(() => sut.Save(model));
			};

			private It should_throw_an_exception =
				() => _expectedException.ShouldBeOfExactType(typeof (ArgumentException));
		}

		public class When_the_socket_is_really_slow : CleaningConcern
		{
			private static IList<TestModel> models;

			private static Exception _expectedException;

			private Establish context = () =>
			{
				CreateSettings("?socketTimeoutMS=1000");

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
				() => _expectedException.ShouldBeOfExactType(typeof(System.IO.IOException));
		}

		public class When_the_socket_is_retried : CleaningConcern
		{
			private static IList<TestModel> models;
			private static string result;

			private Establish context = () =>
			{
				CreateSettings(string.Empty);

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
		public TestableMongoDbWritableReadModelRepository(IMongoDbReadModelSettings settings) : base(settings)
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
				catch (IOException)
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
	}

}
