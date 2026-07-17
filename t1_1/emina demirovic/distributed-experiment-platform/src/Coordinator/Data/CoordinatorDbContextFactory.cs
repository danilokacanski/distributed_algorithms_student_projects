using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Coordinator.Data;

public sealed class CoordinatorDbContextFactory
    : IDesignTimeDbContextFactory<CoordinatorDbContext>
{
    public CoordinatorDbContext CreateDbContext(string[] args)
    {
        var currentDirectory = Directory.GetCurrentDirectory();

        var projectDirectory =
            File.Exists(Path.Combine(
                currentDirectory,
                "Coordinator.csproj"))
                ? currentDirectory
                : Path.Combine(
                    currentDirectory,
                    "src",
                    "Coordinator");

        var databasePath = Path.Combine(
            projectDirectory,
            "Data",
            "coordinator.db");

        var optionsBuilder =
            new DbContextOptionsBuilder<CoordinatorDbContext>();

        optionsBuilder.UseSqlite(
            $"Data Source={databasePath}");

        return new CoordinatorDbContext(
            optionsBuilder.Options);
    }
}