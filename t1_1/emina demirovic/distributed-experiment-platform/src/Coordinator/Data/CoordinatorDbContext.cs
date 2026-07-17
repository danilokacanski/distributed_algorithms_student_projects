using Microsoft.EntityFrameworkCore;

namespace Coordinator.Data;

public sealed class CoordinatorDbContext(
    DbContextOptions<CoordinatorDbContext> options)
    : DbContext(options)
{
    public DbSet<ExperimentEntity> Experiments =>
        Set<ExperimentEntity>();

    public DbSet<ExperimentEventEntity> ExperimentEvents =>
        Set<ExperimentEventEntity>();

    public DbSet<AppliedCommandEntity> AppliedCommands =>
        Set<AppliedCommandEntity>();

    public DbSet<RaftPersistentStateEntity> RaftPersistentStates =>
        Set<RaftPersistentStateEntity>();

    public DbSet<RaftLogEntryEntity> RaftLogEntries =>
        Set<RaftLogEntryEntity>();

    public DbSet<RaftCommitStateEntity> RaftCommitStates =>
        Set<RaftCommitStateEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ExperimentEntity>(entity =>
        {
            entity.HasKey(experiment => experiment.Id);

            entity.Property(experiment => experiment.Name)
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(experiment => experiment.Algorithm)
                .IsRequired()
                .HasMaxLength(100);

            entity.Property(experiment => experiment.Environment)
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(experiment => experiment.TimeoutSeconds)
                .HasDefaultValue(300);
        });

        modelBuilder.Entity<ExperimentEventEntity>(entity =>
        {
            entity.HasKey(experimentEvent => experimentEvent.Id);

            entity.Property(experimentEvent => experimentEvent.Type)
                .HasConversion<string>()
                .HasMaxLength(50);

            entity.Property(experimentEvent => experimentEvent.Details)
                .HasMaxLength(1000);

            entity.HasOne(experimentEvent => experimentEvent.Experiment)
                .WithMany()
                .HasForeignKey(experimentEvent => experimentEvent.ExperimentId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<AppliedCommandEntity>(entity =>
        {
            entity.HasKey(command => command.CommandId);

            entity.Property(command => command.CommandType)
                .IsRequired()
                .HasMaxLength(100);

            entity.HasIndex(command => command.ExperimentId);
        });

        modelBuilder.Entity<RaftPersistentStateEntity>(entity =>
        {
            entity.HasKey(state => state.NodeId);

            entity.Property(state => state.NodeId)
                .IsRequired()
                .HasMaxLength(100);

            entity.Property(state => state.VotedFor)
                .HasMaxLength(100);
        });

        modelBuilder.Entity<RaftLogEntryEntity>(entity =>
        {
            entity.HasKey(entry =>
                new
                {
                    entry.NodeId,
                    entry.LogIndex
                });

            entity.Property(entry =>
                    entry.NodeId)
                .IsRequired()
                .HasMaxLength(100);

            entity.Property(entry =>
                    entry.CommandType)
                .IsRequired()
                .HasMaxLength(100);

            entity.Property(entry =>
                    entry.CommandPayloadJson)
                .IsRequired();

            entity.HasIndex(entry =>
                    new
                    {
                        entry.NodeId,
                        entry.CommandId
                    })
                .IsUnique();
        });

        modelBuilder.Entity<RaftCommitStateEntity>(entity =>
        {
            entity.HasKey(state =>
                state.NodeId);

            entity.Property(state =>
                    state.NodeId)
                .IsRequired()
                .HasMaxLength(100);
        });
    }
}