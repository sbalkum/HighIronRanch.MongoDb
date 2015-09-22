using System;

namespace HighIronRanch.MongoDb
{
    public class CollectionNamer : ICollectionNamer
    {
        public string GetCollectionName(Type type)
        {
            return type.ToString();
        }
    }
}