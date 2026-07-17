using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Coordinator.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddExperimentCancellation : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "CancellationRequested",
                table: "Experiments",
                type: "INTEGER",
                nullable: false,
                defaultValue: false);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "CancellationRequested",
                table: "Experiments");

        }
    }
}
