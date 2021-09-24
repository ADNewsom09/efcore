// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Utilities;
using Microsoft.Extensions.Logging;

namespace Microsoft.EntityFrameworkCore.Diagnostics
{
    /// <summary>
    ///     <para>
    ///         Configures the runtime behavior of events generated by Entity Framework.
    ///         You can set a default behavior and behaviors for each event ID.
    ///     </para>
    ///     <para>
    ///         This class is used within the
    ///         <see cref="DbContextOptionsBuilder.ConfigureWarnings" />
    ///         API and it is not designed to be directly constructed in your application code.
    ///     </para>
    /// </summary>
    /// <remarks>
    ///     See <see href="https://aka.ms/efcore-warning-configuration">Configuration for specific messages</see> for more information.
    /// </remarks>
    public class WarningsConfigurationBuilder
    {
        private readonly DbContextOptionsBuilder _optionsBuilder;

        /// <summary>
        ///     Initializes a new instance of the <see cref="WarningsConfigurationBuilder" /> class.
        /// </summary>
        /// <param name="optionsBuilder">The options builder to which the warnings configuration will be applied.</param>
        public WarningsConfigurationBuilder(DbContextOptionsBuilder optionsBuilder)
        {
            Check.NotNull(optionsBuilder, nameof(optionsBuilder));

            _optionsBuilder = optionsBuilder;
        }

        /// <summary>
        ///     <para>
        ///         Sets the default behavior when a warning is generated.
        ///     </para>
        ///     <para>
        ///         Event ID values can be found in <see cref="CoreEventId" /> and
        ///         <see cref="T:Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId" />.
        ///         The database provider being used may also define provider-specific event IDs in a similar class.
        ///     </para>
        /// </summary>
        /// <remarks>
        ///     See <see href="https://aka.ms/efcore-warning-configuration">Configuration for specific messages</see> for more information.
        /// </remarks>
        /// <param name="warningBehavior">The desired behavior.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public virtual WarningsConfigurationBuilder Default(WarningBehavior warningBehavior)
            => WithOption(e => e.WithDefaultBehavior(warningBehavior));

        /// <summary>
        ///     <para>
        ///         Causes an exception to be thrown when the specified event occurs, regardless of default configuration.
        ///     </para>
        ///     <para>
        ///         Event ID values can be found in <see cref="CoreEventId" /> and
        ///         <see cref="T:Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId" />.
        ///         The database provider being used may also define provider-specific event IDs in a similar class.
        ///     </para>
        /// </summary>
        /// <remarks>
        ///     See <see href="https://aka.ms/efcore-warning-configuration">Configuration for specific messages</see> for more information.
        /// </remarks>
        /// <param name="eventIds">
        ///     The IDs for events to configure.
        /// </param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public virtual WarningsConfigurationBuilder Throw(
            params EventId[] eventIds)
        {
            Check.NotNull(eventIds, nameof(eventIds));

            return WithOption(e => e.WithExplicit(eventIds, WarningBehavior.Throw));
        }

        /// <summary>
        ///     <para>
        ///         Causes an event to be logged, regardless of default configuration.
        ///     </para>
        ///     <para>
        ///         Event ID values can be found in <see cref="CoreEventId" /> and
        ///         <see cref="T:Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId" />.
        ///         The database provider being used may also define provider-specific event IDs in a similar class.
        ///     </para>
        /// </summary>
        /// <remarks>
        ///     See <see href="https://aka.ms/efcore-warning-configuration">Configuration for specific messages</see> for more information.
        /// </remarks>
        /// <param name="eventIds">
        ///     The IDs for events to configure.
        /// </param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public virtual WarningsConfigurationBuilder Log(
            params EventId[] eventIds)
        {
            Check.NotNull(eventIds, nameof(eventIds));

            return WithOption(e => e.WithExplicit(eventIds, WarningBehavior.Log));
        }

        /// <summary>
        ///     <para>
        ///         Causes an event to be logged at the specified level, regardless of default configuration.
        ///     </para>
        ///     <para>
        ///         Event ID values can be found in <see cref="CoreEventId" /> and
        ///         <see cref="T:Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId" />.
        ///         The database provider being used may also define provider-specific event IDs in a similar class.
        ///     </para>
        /// </summary>
        /// <remarks>
        ///     See <see href="https://aka.ms/efcore-warning-configuration">Configuration for specific messages</see> for more information.
        /// </remarks>
        /// <param name="eventsAndLevels">
        ///     The event IDs and levels to configure.
        /// </param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public virtual WarningsConfigurationBuilder Log(
            params (EventId Id, LogLevel Level)[] eventsAndLevels)
        {
            Check.NotNull(eventsAndLevels, nameof(eventsAndLevels));

            return WithOption(e => e.WithExplicit(eventsAndLevels));
        }

        /// <summary>
        ///     <para>
        ///         Causes nothing to happen when the specified event occurs, regardless of default configuration.
        ///     </para>
        ///     <para>
        ///         Event ID values can be found in <see cref="CoreEventId" /> and
        ///         <see cref="T:Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId" />.
        ///         The database provider being used may also define provider-specific event IDs in a similar class.
        ///     </para>
        /// </summary>
        /// <remarks>
        ///     See <see href="https://aka.ms/efcore-warning-configuration">Configuration for specific messages</see> for more information.
        /// </remarks>
        /// <param name="eventIds">
        ///     The IDs for events to configure.
        /// </param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public virtual WarningsConfigurationBuilder Ignore(
            params EventId[] eventIds)
        {
            Check.NotNull(eventIds, nameof(eventIds));

            return WithOption(e => e.WithExplicit(eventIds, WarningBehavior.Ignore));
        }

        private WarningsConfigurationBuilder WithOption(Func<WarningsConfiguration, WarningsConfiguration> withFunc)
        {
            var coreOptionsExtension = _optionsBuilder.Options.FindExtension<CoreOptionsExtension>() ?? new CoreOptionsExtension();

            ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(
                coreOptionsExtension.WithWarningsConfiguration(withFunc(coreOptionsExtension.WarningsConfiguration)));

            return this;
        }
    }
}
