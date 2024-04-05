// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#nullable disable

using Microsoft.EntityFrameworkCore.Cosmos.Internal;

namespace Microsoft.EntityFrameworkCore.Cosmos.Query.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class CosmosQueryableMethodTranslatingExpressionVisitor : QueryableMethodTranslatingExpressionVisitor
{
    private readonly QueryCompilationContext _queryCompilationContext;
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IMemberTranslatorProvider _memberTranslatorProvider;
    private readonly IMethodCallTranslatorProvider _methodCallTranslatorProvider;
    private readonly CosmosSqlTranslatingExpressionVisitor _sqlTranslator;
    private readonly CosmosProjectionBindingExpressionVisitor _projectionBindingExpressionVisitor;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public CosmosQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        ISqlExpressionFactory sqlExpressionFactory,
        IMemberTranslatorProvider memberTranslatorProvider,
        IMethodCallTranslatorProvider methodCallTranslatorProvider)
        : base(dependencies, queryCompilationContext, subquery: false)
    {
        _queryCompilationContext = queryCompilationContext;
        _sqlExpressionFactory = sqlExpressionFactory;
        _memberTranslatorProvider = memberTranslatorProvider;
        _methodCallTranslatorProvider = methodCallTranslatorProvider;
        _sqlTranslator = new CosmosSqlTranslatingExpressionVisitor(
            queryCompilationContext,
            _sqlExpressionFactory,
            _memberTranslatorProvider,
            _methodCallTranslatorProvider);
        _projectionBindingExpressionVisitor =
            new CosmosProjectionBindingExpressionVisitor(_queryCompilationContext.Model, _sqlTranslator);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected CosmosQueryableMethodTranslatingExpressionVisitor(
        CosmosQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor.Dependencies, parentVisitor.QueryCompilationContext, subquery: true)
    {
        _queryCompilationContext = parentVisitor._queryCompilationContext;
        _sqlExpressionFactory = parentVisitor._sqlExpressionFactory;
        _sqlTranslator = new CosmosSqlTranslatingExpressionVisitor(
            QueryCompilationContext,
            _sqlExpressionFactory,
            _memberTranslatorProvider,
            _methodCallTranslatorProvider);
        _projectionBindingExpressionVisitor =
            new CosmosProjectionBindingExpressionVisitor(_queryCompilationContext.Model, _sqlTranslator);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override Expression Visit(Expression expression)
    {
        if (expression is MethodCallExpression { Method.IsGenericMethod: true } methodCallExpression
            && methodCallExpression.Method.GetGenericMethodDefinition() == QueryableMethods.FirstOrDefaultWithoutPredicate)
        {
            if (methodCallExpression.Arguments[0] is MethodCallExpression { Method.IsGenericMethod: true } queryRootMethodCallExpression
                && queryRootMethodCallExpression.Method.GetGenericMethodDefinition() == QueryableMethods.Where)
            {
                if (queryRootMethodCallExpression.Arguments[0] is EntityQueryRootExpression entityQueryRootExpression)
                {
                    var entityType = entityQueryRootExpression.EntityType;

                    if (queryRootMethodCallExpression.Arguments[1] is UnaryExpression { Operand: LambdaExpression lambdaExpression })
                    {
                        var queryProperties = new List<IProperty>();
                        var parameterNames = new List<string>();

                        if (ExtractPartitionKeyFromPredicate(entityType, lambdaExpression.Body, queryProperties, parameterNames))
                        {
                            var entityTypePrimaryKeyProperties = entityType.FindPrimaryKey()!.Properties;
                            var idProperty = entityType.GetProperties()
                                .First(p => p.GetJsonPropertyName() == StoreKeyConvention.IdPropertyJsonName);
                            var partitionKeyProperties = entityType.GetPartitionKeyProperties();

                            if (entityTypePrimaryKeyProperties.SequenceEqual(queryProperties)
                                && (!partitionKeyProperties.Any()
                                    || partitionKeyProperties.All(p => entityTypePrimaryKeyProperties.Contains(p)))
                                && (idProperty.GetValueGeneratorFactory() != null
                                    || entityTypePrimaryKeyProperties.Contains(idProperty)))
                            {
                                var propertyParameterList = queryProperties.Zip(
                                        parameterNames,
                                        (property, parameter) => (property, parameter))
                                    .ToDictionary(tuple => tuple.property, tuple => tuple.parameter);

                                var readItemExpression = new ReadItemExpression(entityType, propertyParameterList);

                                return CreateShapedQueryExpression(entityType, readItemExpression)
                                    .UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            }
                        }
                    }
                }
            }
        }

        return base.Visit(expression);

        static bool ExtractPartitionKeyFromPredicate(
            IEntityType entityType,
            Expression joinCondition,
            ICollection<IProperty> properties,
            ICollection<string> parameterNames)
        {
            if (joinCondition is BinaryExpression joinBinaryExpression)
            {
                if (joinBinaryExpression.NodeType == ExpressionType.AndAlso)
                {
                    return ExtractPartitionKeyFromPredicate(entityType, joinBinaryExpression.Left, properties, parameterNames)
                        && ExtractPartitionKeyFromPredicate(entityType, joinBinaryExpression.Right, properties, parameterNames);
                }

                if (joinBinaryExpression.NodeType == ExpressionType.Equal
                    && joinBinaryExpression.Left is MethodCallExpression equalMethodCallExpression
                    && joinBinaryExpression.Right is ParameterExpression equalParameterExpresion
                    && equalMethodCallExpression.TryGetEFPropertyArguments(out _, out var propertyName))
                {
                    var property = entityType.FindProperty(propertyName);
                    if (property == null)
                    {
                        return false;
                    }

                    properties.Add(property);
                    parameterNames.Add(equalParameterExpresion.Name);
                    return true;
                }
            }
            else if (joinCondition is MethodCallExpression
                     {
                         Method.Name: "Equals",
                         Object: null,
                         Arguments: [MethodCallExpression equalsMethodCallExpression, ParameterExpression parameterExpresion]
                     }
                     && equalsMethodCallExpression.TryGetEFPropertyArguments(out _, out var propertyName))
            {
                var property = entityType.FindProperty(propertyName);
                if (property == null)
                {
                    return false;
                }

                properties.Add(property);
                parameterNames.Add(parameterExpresion.Name);
                return true;
            }

            return false;
        }
    }

    /// <inheritdoc />
    protected override Expression VisitExtension(Expression extensionExpression)
    {
        switch (extensionExpression)
        {
            case FromSqlQueryRootExpression fromSqlQueryRootExpression:
                return CreateShapedQueryExpression(
                    fromSqlQueryRootExpression.EntityType,
                    _sqlExpressionFactory.Select(
                        fromSqlQueryRootExpression.EntityType,
                        fromSqlQueryRootExpression.Sql,
                        fromSqlQueryRootExpression.Argument));

            default:
                return base.VisitExtension(extensionExpression);
        }
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new CosmosQueryableMethodTranslatingExpressionVisitor(this);

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override ShapedQueryExpression TranslateSubquery(Expression expression)
        => throw new InvalidOperationException(CoreStrings.TranslationFailed(expression.Print()));

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression CreateShapedQueryExpression(IEntityType entityType)
    {
        var selectExpression = _sqlExpressionFactory.Select(entityType);

        return CreateShapedQueryExpression(entityType, selectExpression);
    }

    private static ShapedQueryExpression CreateShapedQueryExpression(IEntityType entityType, Expression queryExpression)
        => new(
            queryExpression,
            new StructuralTypeShaperExpression(
                entityType,
                new ProjectionBindingExpression(queryExpression, new ProjectionMember(), typeof(ValueBuffer)),
                false));

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateAll(ShapedQueryExpression source, LambdaExpression predicate)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateAny(ShapedQueryExpression source, LambdaExpression predicate)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateAverage(ShapedQueryExpression source, LambdaExpression selector, Type resultType)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct
            || selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            return null;
        }

        if (selector != null)
        {
            source = TranslateSelect(source, selector);
        }

        var projection = (SqlExpression)selectExpression.GetMappedProjection(new ProjectionMember());
        projection = _sqlExpressionFactory.Function(
            "AVG", new[] { projection }, projection.Type, projection.TypeMapping);

        return AggregateResultShaper(source, projection, throwOnNullResult: true, resultType);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateCast(ShapedQueryExpression source, Type resultType)
        => source.ShaperExpression.Type != resultType
            ? source.UpdateShaperExpression(Expression.Convert(source.ShaperExpression, resultType))
            : source;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateConcat(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateContains(ShapedQueryExpression source, Expression item)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateCount(ShapedQueryExpression source, LambdaExpression predicate)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct
            || selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            return null;
        }

        if (predicate != null)
        {
            source = TranslateWhere(source, predicate);
            if (source == null)
            {
                return null;
            }
        }

        var translation = _sqlExpressionFactory.ApplyDefaultTypeMapping(
            _sqlExpressionFactory.Function("COUNT", new[] { _sqlExpressionFactory.Constant(1) }, typeof(int)));

        var projectionMapping = new Dictionary<ProjectionMember, Expression> { { new ProjectionMember(), translation } };

        selectExpression.ClearOrdering();
        selectExpression.ReplaceProjectionMapping(projectionMapping);
        return source.UpdateShaperExpression(
            Expression.Convert(
                new ProjectionBindingExpression(source.QueryExpression, new ProjectionMember(), typeof(int?)),
                typeof(int)));
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateDefaultIfEmpty(ShapedQueryExpression source, Expression defaultValue)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateDistinct(ShapedQueryExpression source)
    {
        ((SelectExpression)source.QueryExpression).ApplyDistinct();

        return source;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateElementAtOrDefault(
        ShapedQueryExpression source,
        Expression index,
        bool returnDefault)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateExcept(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateFirstOrDefault(
        ShapedQueryExpression source,
        LambdaExpression predicate,
        Type returnType,
        bool returnDefault)
    {
        if (predicate != null)
        {
            source = TranslateWhere(source, predicate);
            if (source == null)
            {
                return null;
            }
        }

        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.Predicate == null
            && selectExpression.Orderings.Count == 0)
        {
            _queryCompilationContext.Logger.FirstWithoutOrderByAndFilterWarning();
        }

        selectExpression.ApplyLimit(TranslateExpression(Expression.Constant(1)));

        return source.ShaperExpression.Type != returnType
            ? source.UpdateShaperExpression(Expression.Convert(source.ShaperExpression, returnType))
            : source;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateGroupBy(
        ShapedQueryExpression source,
        LambdaExpression keySelector,
        LambdaExpression elementSelector,
        LambdaExpression resultSelector)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateGroupJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateIntersect(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateLastOrDefault(
        ShapedQueryExpression source,
        LambdaExpression predicate,
        Type returnType,
        bool returnDefault)
    {
        if (predicate != null)
        {
            source = TranslateWhere(source, predicate);
            if (source == null)
            {
                return null;
            }
        }

        var selectExpression = (SelectExpression)source.QueryExpression;
        selectExpression.ReverseOrderings();
        selectExpression.ApplyLimit(TranslateExpression(Expression.Constant(1)));

        return source.ShaperExpression.Type != returnType
            ? source.UpdateShaperExpression(Expression.Convert(source.ShaperExpression, returnType))
            : source;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateLeftJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateLongCount(ShapedQueryExpression source, LambdaExpression predicate)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct
            || selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            return null;
        }

        if (predicate != null)
        {
            source = TranslateWhere(source, predicate);
            if (source == null)
            {
                return null;
            }
        }

        var translation = _sqlExpressionFactory.ApplyDefaultTypeMapping(
            _sqlExpressionFactory.Function("COUNT", new[] { _sqlExpressionFactory.Constant(1) }, typeof(long)));
        var projectionMapping = new Dictionary<ProjectionMember, Expression> { { new ProjectionMember(), translation } };

        selectExpression.ClearOrdering();
        selectExpression.ReplaceProjectionMapping(projectionMapping);
        return source.UpdateShaperExpression(
            Expression.Convert(
                new ProjectionBindingExpression(source.QueryExpression, new ProjectionMember(), typeof(long?)),
                typeof(long)));
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateMax(ShapedQueryExpression source, LambdaExpression selector, Type resultType)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct
            || selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            return null;
        }

        if (selector != null)
        {
            source = TranslateSelect(source, selector);
        }

        var projection = (SqlExpression)selectExpression.GetMappedProjection(new ProjectionMember());

        projection = _sqlExpressionFactory.Function("MAX", new[] { projection }, resultType, projection.TypeMapping);

        return AggregateResultShaper(source, projection, throwOnNullResult: true, resultType);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateMin(ShapedQueryExpression source, LambdaExpression selector, Type resultType)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct
            || selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            return null;
        }

        if (selector != null)
        {
            source = TranslateSelect(source, selector);
        }

        var projection = (SqlExpression)selectExpression.GetMappedProjection(new ProjectionMember());

        projection = _sqlExpressionFactory.Function("MIN", new[] { projection }, resultType, projection.TypeMapping);

        return AggregateResultShaper(source, projection, throwOnNullResult: true, resultType);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateOfType(ShapedQueryExpression source, Type resultType)
    {
        if (source.ShaperExpression is StructuralTypeShaperExpression entityShaperExpression)
        {
            if (entityShaperExpression.StructuralType is not IEntityType entityType)
            {
                throw new UnreachableException("Complex types not supported in Cosmos");
            }

            if (entityType.ClrType == resultType)
            {
                return source;
            }

            var parameterExpression = Expression.Parameter(entityShaperExpression.Type);
            var predicate = Expression.Lambda(Expression.TypeIs(parameterExpression, resultType), parameterExpression);
            var translation = TranslateLambdaExpression(source, predicate);
            if (translation == null)
            {
                // EntityType is not part of hierarchy
                return null;
            }

            var selectExpression = (SelectExpression)source.QueryExpression;
            if (translation is not SqlConstantExpression { Value: true })
            {
                selectExpression.ApplyPredicate(translation);
            }

            var baseType = entityType.GetAllBaseTypes().SingleOrDefault(et => et.ClrType == resultType);
            if (baseType != null)
            {
                return source.UpdateShaperExpression(entityShaperExpression.WithType(baseType));
            }

            var derivedType = entityType.GetDerivedTypes().Single(et => et.ClrType == resultType);
            var projectionBindingExpression = (ProjectionBindingExpression)entityShaperExpression.ValueBufferExpression;

            var projectionMember = projectionBindingExpression.ProjectionMember;
            Check.DebugAssert(new ProjectionMember().Equals(projectionMember), "Invalid ProjectionMember when processing OfType");

            var entityProjectionExpression = (EntityProjectionExpression)selectExpression.GetMappedProjection(projectionMember);
            selectExpression.ReplaceProjectionMapping(
                new Dictionary<ProjectionMember, Expression>
                {
                    { projectionMember, entityProjectionExpression.UpdateEntityType(derivedType) }
                });

            return source.UpdateShaperExpression(entityShaperExpression.WithType(derivedType));
        }

        return null;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateOrderBy(
        ShapedQueryExpression source,
        LambdaExpression keySelector,
        bool ascending)
    {
        var translation = TranslateLambdaExpression(source, keySelector);
        if (translation != null)
        {
            ((SelectExpression)source.QueryExpression).ApplyOrdering(new OrderingExpression(translation, ascending));

            return source;
        }

        return null;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateReverse(ShapedQueryExpression source)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.Orderings.Count == 0)
        {
            AddTranslationErrorDetails(CosmosStrings.MissingOrderingInSelectExpression);
            return null;
        }

        selectExpression.ReverseOrderings();

        return source;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSelect(ShapedQueryExpression source, LambdaExpression selector)
    {
        if (selector.Body == selector.Parameters[0])
        {
            return source;
        }

        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct)
        {
            return null;
        }

        var newSelectorBody = ReplacingExpressionVisitor.Replace(selector.Parameters.Single(), source.ShaperExpression, selector.Body);

        return source.UpdateShaperExpression(_projectionBindingExpressionVisitor.Translate(selectExpression, newSelectorBody));
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSelectMany(
        ShapedQueryExpression source,
        LambdaExpression collectionSelector,
        LambdaExpression resultSelector)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSelectMany(ShapedQueryExpression source, LambdaExpression selector)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSingleOrDefault(
        ShapedQueryExpression source,
        LambdaExpression predicate,
        Type returnType,
        bool returnDefault)
    {
        if (predicate != null)
        {
            source = TranslateWhere(source, predicate);
            if (source == null)
            {
                return null;
            }
        }

        var selectExpression = (SelectExpression)source.QueryExpression;
        selectExpression.ApplyLimit(TranslateExpression(Expression.Constant(2)));

        return source.ShaperExpression.Type != returnType
            ? source.UpdateShaperExpression(Expression.Convert(source.ShaperExpression, returnType))
            : source;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSkip(ShapedQueryExpression source, Expression count)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        var translation = TranslateExpression(count);

        if (translation != null)
        {
            if (selectExpression.Orderings.Count == 0)
            {
                _queryCompilationContext.Logger.RowLimitingOperationWithoutOrderByWarning();
            }

            selectExpression.ApplyOffset(translation);

            return source;
        }

        return null;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSkipWhile(ShapedQueryExpression source, LambdaExpression predicate)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateSum(ShapedQueryExpression source, LambdaExpression selector, Type resultType)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        if (selectExpression.IsDistinct
            || selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            return null;
        }

        if (selector != null)
        {
            source = TranslateSelect(source, selector);
        }

        var serverOutputType = resultType.UnwrapNullableType();
        var projection = (SqlExpression)selectExpression.GetMappedProjection(new ProjectionMember());

        projection = _sqlExpressionFactory.Function(
            "SUM", new[] { projection }, serverOutputType, projection.TypeMapping);

        return AggregateResultShaper(source, projection, throwOnNullResult: false, resultType);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateTake(ShapedQueryExpression source, Expression count)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        var translation = TranslateExpression(count);

        if (translation != null)
        {
            if (selectExpression.Orderings.Count == 0)
            {
                _queryCompilationContext.Logger.RowLimitingOperationWithoutOrderByWarning();
            }

            selectExpression.ApplyLimit(translation);

            return source;
        }

        return null;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateTakeWhile(ShapedQueryExpression source, LambdaExpression predicate)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateThenBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
    {
        var translation = TranslateLambdaExpression(source, keySelector);
        if (translation != null)
        {
            ((SelectExpression)source.QueryExpression).AppendOrdering(new OrderingExpression(translation, ascending));

            return source;
        }

        return null;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateUnion(ShapedQueryExpression source1, ShapedQueryExpression source2)
        => null;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override ShapedQueryExpression TranslateWhere(ShapedQueryExpression source, LambdaExpression predicate)
    {
        if (source.ShaperExpression is StructuralTypeShaperExpression { StructuralType: IEntityType entityType } entityShaperExpression
            && entityType.GetPartitionKeyPropertyNames().FirstOrDefault() != null)
        {
            List<(Expression, IProperty)> partitionKeyValues = new();
            if (TryExtractPartitionKey(predicate.Body, entityType, out var newPredicate, partitionKeyValues))
            {
                foreach (var propertyName in entityType.GetPartitionKeyPropertyNames())
                {
                    var partitionKeyValue = partitionKeyValues.FirstOrDefault(p => p.Item2.Name == propertyName);
                    ((SelectExpression)source.QueryExpression).AddPartitionKey(partitionKeyValue.Item2, partitionKeyValue.Item1);
                }

                if (newPredicate == null)
                {
                    return source;
                }

                predicate = Expression.Lambda(newPredicate, predicate.Parameters);
            }
        }

        var translation = TranslateLambdaExpression(source, predicate);
        if (translation != null)
        {
            ((SelectExpression)source.QueryExpression).ApplyPredicate(translation);

            return source;
        }

        return null;

        bool TryExtractPartitionKey(
            Expression expression, IEntityType entityType, out Expression updatedPredicate, List<(Expression, IProperty)> partitionKeyValues)
        {
            updatedPredicate = null;
            if (expression is BinaryExpression binaryExpression)
            {
                if (TryGetPartitionKeyValue(binaryExpression, entityType, out var valueExpression, out var property))
                {
                    partitionKeyValues.Add((valueExpression, property));
                    return true;
                }

                if (binaryExpression.NodeType == ExpressionType.AndAlso)
                {
                    var foundInRight = TryExtractPartitionKey(
                        binaryExpression.Left, entityType, out var leftPredicate, partitionKeyValues);

                    var foundInLeft = TryExtractPartitionKey(
                        binaryExpression.Right, entityType, out var rightPredicate, partitionKeyValues);

                    if (foundInLeft && foundInRight)
                    {
                        return true;
                    }

                    if (foundInLeft || foundInRight)
                    {
                        updatedPredicate = leftPredicate != null
                            ? rightPredicate != null
                                ? binaryExpression.Update(leftPredicate, binaryExpression.Conversion, rightPredicate)
                                : leftPredicate
                            : rightPredicate;

                        return true;
                    }
                }
            }
            else if (expression.NodeType == ExpressionType.MemberAccess
                     && expression.Type == typeof(bool))
            {
                if (IsPartitionKeyPropertyAccess(expression, entityType, out var property))
                {
                    partitionKeyValues.Add((Expression.Constant(true), property));
                    return true;
                }

            }
            else if (expression.NodeType == ExpressionType.Not)
            {
                if (IsPartitionKeyPropertyAccess(((UnaryExpression)expression).Operand, entityType, out var property))
                {
                    partitionKeyValues.Add((Expression.Constant(false), property));
                    return true;
                }
            }

            updatedPredicate = expression;
            return false;
        }

        bool TryGetPartitionKeyValue(BinaryExpression binaryExpression, IEntityType entityType, out Expression expression, out IProperty property)
        {
            if (binaryExpression.NodeType == ExpressionType.Equal)
            {
                expression = IsPartitionKeyPropertyAccess(binaryExpression.Left, entityType, out property)
                    ? binaryExpression.Right
                    : IsPartitionKeyPropertyAccess(binaryExpression.Right, entityType, out property)
                        ? binaryExpression.Left
                        : null;

                if (expression is ConstantExpression
                    || (expression is ParameterExpression valueParameterExpression
                        && valueParameterExpression.Name?
                            .StartsWith(QueryCompilationContext.QueryParameterPrefix, StringComparison.Ordinal)
                        == true))
                {
                    return true;
                }
            }

            expression = null;
            property = null;
            return false;
        }

        bool IsPartitionKeyPropertyAccess(Expression expression, IEntityType entityType, out IProperty property)
        {
            property = null;
            switch (expression)
            {
                case MemberExpression memberExpression:
                    property = entityType.FindProperty(memberExpression.Member.GetSimpleMemberName());
                    break;

                case MethodCallExpression methodCallExpression
                    when methodCallExpression.TryGetEFPropertyArguments(out _, out var propertyName):
                    property = entityType.FindProperty(propertyName);
                    break;

                case MethodCallExpression methodCallExpression
                    when methodCallExpression.TryGetIndexerArguments(_queryCompilationContext.Model, out _, out var propertyName):
                    property = entityType.FindProperty(propertyName);
                    break;
            }

            return property != null && entityType.GetPartitionKeyPropertyNames().Contains(property.Name);
        }
    }

    private SqlExpression TranslateExpression(Expression expression)
    {
        var translation = _sqlTranslator.Translate(expression);
        if (translation == null && _sqlTranslator.TranslationErrorDetails != null)
        {
            AddTranslationErrorDetails(_sqlTranslator.TranslationErrorDetails);
        }

        return translation;
    }

    private SqlExpression TranslateLambdaExpression(
        ShapedQueryExpression shapedQueryExpression,
        LambdaExpression lambdaExpression)
    {
        var lambdaBody = RemapLambdaBody(shapedQueryExpression.ShaperExpression, lambdaExpression);

        return TranslateExpression(lambdaBody);
    }

    private static Expression RemapLambdaBody(Expression shaperBody, LambdaExpression lambdaExpression)
        => ReplacingExpressionVisitor.Replace(lambdaExpression.Parameters.Single(), shaperBody, lambdaExpression.Body);

    private static ShapedQueryExpression AggregateResultShaper(
        ShapedQueryExpression source,
        Expression projection,
        bool throwOnNullResult,
        Type resultType)
    {
        var selectExpression = (SelectExpression)source.QueryExpression;
        selectExpression.ReplaceProjectionMapping(
            new Dictionary<ProjectionMember, Expression> { { new ProjectionMember(), projection } });

        selectExpression.ClearOrdering();

        var nullableResultType = resultType.MakeNullable();
        Expression shaper = new ProjectionBindingExpression(source.QueryExpression, new ProjectionMember(), nullableResultType);

        if (throwOnNullResult)
        {
            var resultVariable = Expression.Variable(nullableResultType, "result");
            var returnValueForNull = resultType.IsNullableType()
                ? (Expression)Expression.Constant(null, resultType)
                : Expression.Throw(
                    Expression.New(
                        typeof(InvalidOperationException).GetConstructors()
                            .Single(ci => ci.GetParameters().Length == 1),
                        Expression.Constant(CoreStrings.SequenceContainsNoElements)),
                    resultType);

            shaper = Expression.Block(
                new[] { resultVariable },
                Expression.Assign(resultVariable, shaper),
                Expression.Condition(
                    Expression.Equal(resultVariable, Expression.Default(nullableResultType)),
                    returnValueForNull,
                    resultType != resultVariable.Type
                        ? Expression.Convert(resultVariable, resultType)
                        : resultVariable));
        }
        else if (resultType != shaper.Type)
        {
            shaper = Expression.Convert(shaper, resultType);
        }

        return source.UpdateShaperExpression(shaper);
    }
}
