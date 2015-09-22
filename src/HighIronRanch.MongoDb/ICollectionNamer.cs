using System;

namespace HighIronRanch.MongoDb
{
    public interface ICollectionNamer
    {
        string GetCollectionName(Type type);
    }
}