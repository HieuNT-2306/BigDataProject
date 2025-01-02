import React, { useState } from "react";
import listCard from "./cards";
import images from './getImages'; // Import the dynamic image object

export default function Recommend_web() {
    const [chosenCardIDs, setChosenCardIDs] = useState([]); // State for chosen card IDs
    const [recommendationNums, setRecommendationNums] = useState(0); // State for recommendation number
    const [searchQuery, setSearchQuery] = useState(""); // State for search query
    const [apiResponse, setApiResponse] = useState({
        recommendations: [
            {
                card_id: 28000001,
                score: 0.9997851508459223,
                timestamp: "2025-01-02T22:30:41.961423",
            },
        ],
        model_timestamp: "2025-01-02T15:29:07.937000",
    }); // Pre-filled with sample response

    // Function to handle the toggling of card IDs
    const handleToggleCard = (cardId) => {
        setChosenCardIDs((prevIDs) => {
            if (prevIDs.includes(cardId)) {
                return prevIDs.filter((id) => id !== cardId);
            } else {
                return [...prevIDs, cardId];
            }
        });
    };

    // Function to handle input change for recommendations
    const handleInputChange = (e) => {
        const value = parseInt(e.target.value, 10);
        setRecommendationNums(isNaN(value) ? 0 : value);
    };

    // Function to handle search input change
    const handleSearchChange = (e) => {
        setSearchQuery(e.target.value);
    };

    // Filtered cards based on search query
    const filteredCards = listCard.filter((card) =>
        card.name.toLowerCase().includes(searchQuery.toLowerCase())
    );

    const sendApiRequest = async () => {
        const formData = new FormData();
        chosenCardIDs.forEach((id) => {
            formData.append("partial_deck", id);
        });
        formData.append("recommendation_nums", recommendationNums);

        try {
            const response = await fetch("http://localhost:8000/recommendation", {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }

            const data = await response.json();
            setApiResponse(data); // Save the response to state
        } catch (error) {
            console.error("Error occurred while sending API request:", error);
        }
    };

    return (
        <div className="p-6">
            {/* API Response Section */}
            <div className="p-4 border rounded-lg bg-gray-50 shadow-md mb-6">
                <h3 className="text-lg font-semibold mb-2 text-gray-700">API Response:</h3>
                <pre className="text-sm bg-gray-100 p-4 rounded-lg overflow-auto">
                    {JSON.stringify(apiResponse, null, 2)}
                </pre>
            </div>

            {/* Display chosenCardIDs as images */}
            <div className="mt-4 p-4 border rounded-lg bg-gray-50 shadow-md">
                <h3 className="text-lg font-semibold mb-2 text-gray-700">Chosen Cards:</h3>
                <div className="grid grid-cols-3 gap-2">
                    {chosenCardIDs.length > 0 ? (
                        chosenCardIDs.map((id, index) => {
                            const card = listCard.find((card) => card.id === id);
                            return (
                                <div key={index} className="p-2 bg-blue-100 text-blue-900 text-sm font-medium rounded-lg text-center">
                                    {/* Display the image corresponding to the card */}
                                    <img
                                        src={images[`${card.key}.png`]} // Dynamically get the image
                                        alt={`Card Image for ${card.name}`}
                                        className="h-16 w-16 object-cover rounded-md mx-auto"
                                    />
                                </div>
                            );
                        })
                    ) : (
                        <p className="text-sm text-gray-500 col-span-3">
                            No cards selected yet. Click "Add to deck" to choose cards.
                        </p>
                    )}
                </div>
            </div>

            {/* Input for recommendation numbers */}
            <div className="mt-4">
                <label
                    htmlFor="recommendationNums"
                    className="block text-sm font-medium text-gray-700"
                >
                    Number of Recommendations:
                </label>
                <input
                    id="recommendationNums"
                    type="number"
                    value={recommendationNums}
                    onChange={handleInputChange}
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                />
            </div>

            {/* Send API Request Button */}
            <div className="mt-4">
                <button
                    onClick={sendApiRequest}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
                >
                    Send API Request
                </button>
            </div>

            {/* Search Bar for Card Names */}
            <div className="mt-4">
                <label htmlFor="searchQuery" className="block text-sm font-medium text-gray-700">
                    Search Card Name:
                </label>
                <input
                    id="searchQuery"
                    type="text"
                    value={searchQuery}
                    onChange={handleSearchChange}
                    placeholder="Search for a card..."
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                />
            </div>

            {/* Table displaying recommended cards from API response */}
            <div className="relative overflow-x-auto shadow-md sm:rounded-lg mt-6">
                <table className="w-full text-sm text-left rtl:text-right text-gray-500 dark:text-gray-400">
                    <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
                        <tr>
                            <th scope="col" className="px-6 py-3">Card ID</th>
                            <th scope="col" className="px-6 py-3">Card Name</th>
                            <th scope="col" className="px-6 py-3">Card Image</th>
                            <th scope="col" className="px-6 py-3">Score</th>
                            <th scope="col" className="px-6 py-3">Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {apiResponse.recommendations.map((recommendation, index) => {
                            const card = listCard.find((card) => card.id === recommendation.card_id);
                            return (
                                <tr
                                    key={index}
                                    className="odd:bg-white odd:dark:bg-gray-900 even:bg-gray-50 even:dark:bg-gray-800 border-b dark:border-gray-700"
                                >
                                    <th
                                        scope="row"
                                        className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white"
                                    >
                                        {card.id}
                                    </th>
                                    <td className="px-6 py-4">{card.name}</td>
                                    <td className="px-6 py-4">
                                        <img
                                            src={images[`${card.key}.png`]} // Dynamically reference the image
                                            alt={`Card Image for ${card.name}`}
                                            className="h-16 w-16 object-cover rounded-md"
                                        />
                                    </td>
                                    <td className="px-6 py-4">{recommendation.score}</td>
                                    <td className="px-6 py-4">
                                        <button
                                            className={`font-medium ${chosenCardIDs.includes(card.id)
                                                ? "text-gray-500"
                                                : "text-blue-600 hover:underline"
                                                }`}
                                            onClick={() => handleToggleCard(card.id)}
                                        >
                                            {chosenCardIDs.includes(card.id) ? "Added" : "Add to deck"}
                                        </button>
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
