using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Coordinator.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddExperimentSpecification : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Algorithm",
                table: "Experiments",
                type: "TEXT",
                maxLength: 100,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "Environment",
                table: "Experiments",
                type: "TEXT",
                maxLength: 200,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<int>(
                name: "MaxSteps",
                table: "Experiments",
                type: "INTEGER",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "Priority",
                table: "Experiments",
                type: "INTEGER",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "Seed",
                table: "Experiments",
                type: "INTEGER",
                nullable: false,
                defaultValue: 0);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Algorithm",
                table: "Experiments");

            migrationBuilder.DropColumn(
                name: "Environment",
                table: "Experiments");

            migrationBuilder.DropColumn(
                name: "MaxSteps",
                table: "Experiments");

            migrationBuilder.DropColumn(
                name: "Priority",
                table: "Experiments");

            migrationBuilder.DropColumn(
                name: "Seed",
                table: "Experiments");
        }
    }
}
