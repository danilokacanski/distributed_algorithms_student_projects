using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Coordinator.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddRaftPersistentState : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "RaftPersistentStates",
                columns: table => new
                {
                    NodeId = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    CurrentTerm = table.Column<long>(type: "INTEGER", nullable: false),
                    VotedFor = table.Column<string>(type: "TEXT", maxLength: 100, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RaftPersistentStates", x => x.NodeId);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "RaftPersistentStates");
        }
    }
}
